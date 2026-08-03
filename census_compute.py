#!/usr/bin/env python3
"""Build and verify a free, auditable Irish Shopify census.

This public compute worker unions independent public candidate sources, checks
Shopify's public storefront metadata, canonicalises aliases by shop ID/tenant,
and fails closed below the configured benchmark.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import re
import time
import zipfile
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import duckdb
import requests

SHEET_ID = "1dykrF5EKpQliD4uPNywyYQMzbqMT_iwVOo_bUMbDHB0"
SHEET_URLS = [
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=0",
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&gid=0",
]
IE_LIST_URLS = [
    ("domains_monitor_text", "https://domains-monitor.com/download/ie/text/"),
    ("domains_monitor_zip", "https://domains-monitor.com/download/ie/"),
    ("domainmetadata_active", "https://domainmetadata.com/download/ie/ie-domains-active.zip"),
]
COLLINFO = "https://index.commoncrawl.org/collinfo.json"
CC_ROOT = "https://data.commoncrawl.org/"
UA = "Mozilla/5.0 EirLink-Irish-Shopify-Census/2.0"
DOMAIN_RE = re.compile(r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$", re.I)
TENANT_RE = re.compile(r"^[a-z0-9][a-z0-9-]*\.myshopify\.com$", re.I)
IE_RE = re.compile(r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+ie$", re.I)
IRELAND_VALUES = {"IE", "IRELAND", "IRL", "REPUBLIC OF IRELAND", "EIRE", "ÉIRE"}


def normalise_domain(value: str) -> str:
    value = (value or "").strip().casefold().rstrip(".")
    if not value:
        return ""
    if "://" not in value:
        value = "https://" + value
    try:
        host = (urlparse(value).hostname or "").casefold().rstrip(".")
    except ValueError:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host if DOMAIN_RE.fullmatch(host) else ""


def add_seed(seeds: dict[str, set[str]], domain: str, source: str) -> None:
    domain = normalise_domain(domain)
    if domain:
        seeds[domain].add(source)


def download_public_sheet(session: requests.Session, output_dir: Path, minimum: int) -> set[str]:
    raw = output_dir / "public_sheet_raw.csv"
    last_error = ""
    for url in SHEET_URLS:
        for attempt in range(5):
            try:
                with session.get(url, stream=True, timeout=(30, 300), allow_redirects=True) as response:
                    response.raise_for_status()
                    total = 0
                    with raw.open("wb") as handle:
                        for chunk in response.iter_content(1024 * 1024):
                            if chunk:
                                handle.write(chunk)
                                total += len(chunk)
                    if total < 1_000_000:
                        raise RuntimeError(f"truncated sheet export: {total} bytes")
                domains: set[str] = set()
                with raw.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
                    for row in csv.reader(handle):
                        for cell in row:
                            domain = normalise_domain(cell)
                            if domain:
                                domains.add(domain)
                if len(domains) < minimum:
                    raise RuntimeError(f"sheet contained only {len(domains)} domains")
                return domains
            except (requests.RequestException, OSError, RuntimeError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                time.sleep(min(30, 2 ** attempt))
    raise RuntimeError(last_error or "public sheet download failed")


def domains_from_payload(payload: bytes, content_type: str) -> set[str]:
    domains: set[str] = set()
    texts: list[str] = []
    if payload.startswith(b"PK\x03\x04") or "zip" in content_type.casefold():
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            for name in archive.namelist():
                if not name.endswith("/"):
                    texts.append(archive.read(name).decode("utf-8-sig", errors="replace"))
    else:
        texts.append(payload.decode("utf-8-sig", errors="replace"))
    for text in texts:
        for token in re.split(r"[,;\t\s]+", text):
            domain = normalise_domain(token)
            if domain and IE_RE.fullmatch(domain):
                domains.add(domain)
    return domains


def download_public_ie_list(session: requests.Session, minimum: int) -> tuple[set[str], str, list[dict]]:
    best: set[str] = set()
    best_source = ""
    attempts: list[dict] = []
    for source, url in IE_LIST_URLS:
        for attempt in range(4):
            try:
                response = session.get(url, timeout=(30, 300), allow_redirects=True)
                record = {
                    "source": source,
                    "url": url,
                    "status": response.status_code,
                    "final_url": str(response.url),
                    "bytes": len(response.content),
                    "content_type": response.headers.get("content-type", ""),
                }
                domains = domains_from_payload(response.content, record["content_type"]) if response.status_code == 200 else set()
                record["domains"] = len(domains)
                attempts.append(record)
                if len(domains) > len(best):
                    best, best_source = domains, source
                if len(best) >= minimum:
                    return best, best_source, attempts
            except Exception as exc:
                attempts.append({"source": source, "url": url, "error": f"{type(exc).__name__}: {exc}"})
            time.sleep(min(20, 2 ** attempt))
    return best, best_source, attempts


def cc_paths(session: requests.Session, crawl: str) -> list[str]:
    response = session.get(f"{CC_ROOT}crawl-data/{crawl}/cc-index-table.paths.gz", timeout=120)
    response.raise_for_status()
    text = gzip.decompress(response.content).decode("utf-8")
    paths = [line.strip() for line in text.splitlines() if f"crawl={crawl}/subset=warc/" in line]
    if not paths:
        raise RuntimeError(f"no WARC Parquet paths for {crawl}")
    return paths


def query_cc(crawl: str, paths: list[str], work_dir: Path) -> tuple[set[str], set[str], dict]:
    urls = [CC_ROOT + path for path in paths]
    db = work_dir / f"{crawl}.duckdb"
    con = duckdb.connect(str(db))
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")
    schema = con.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false) LIMIT 0", [urls[0]]).fetchall()
    columns = {row[0] for row in schema}
    host_registered = "url_host_registered_domain" if "url_host_registered_domain" in columns else "url_host_name"
    started = time.time()
    tenant_rows = con.execute(
        """
        SELECT DISTINCT lower(url_host_name)
        FROM read_parquet(?, hive_partitioning=false, union_by_name=true)
        WHERE url_surtkey >= 'com,myshopify,'
          AND url_surtkey < 'com,myshopify-'
          AND fetch_status = 200
          AND lower(url_host_name) LIKE '%.myshopify.com'
        """,
        [urls],
    ).fetchall()
    ie_rows = con.execute(
        f"""
        SELECT DISTINCT lower({host_registered})
        FROM read_parquet(?, hive_partitioning=false, union_by_name=true)
        WHERE url_surtkey >= 'ie,'
          AND url_surtkey < 'ie-'
          AND fetch_status = 200
          AND lower({host_registered}) LIKE '%.ie'
        """,
        [urls],
    ).fetchall()
    con.close()
    tenants = {str(row[0]).casefold() for row in tenant_rows if row and TENANT_RE.fullmatch(str(row[0]))}
    ie_domains = {str(row[0]).casefold() for row in ie_rows if row and IE_RE.fullmatch(str(row[0]))}
    return tenants, ie_domains, {
        "crawl": crawl,
        "parquet_files": len(paths),
        "tenants": len(tenants),
        "ie_domains": len(ie_domains),
        "elapsed_seconds": round(time.time() - started, 3),
    }


def build(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    seeds: dict[str, set[str]] = defaultdict(set)
    summary: dict = {"sources": {}, "commoncrawl": []}

    sheet = download_public_sheet(session, output_dir, args.minimum_sheet)
    for domain in sheet:
        add_seed(seeds, domain, "public_shopify_sheet_465k")
    summary["sources"]["public_sheet"] = len(sheet)

    ie_public, ie_source, ie_attempts = download_public_ie_list(session, args.minimum_ie_list)
    for domain in ie_public:
        add_seed(seeds, domain, f"public_ie_{ie_source or 'fallback'}")
    summary["sources"]["public_ie"] = len(ie_public)
    summary["public_ie_source"] = ie_source
    summary["public_ie_attempts"] = ie_attempts

    response = session.get(COLLINFO, timeout=60)
    response.raise_for_status()
    crawls = [item["id"] for item in response.json()[: args.collections]]
    successful = 0
    for crawl in crawls:
        work = output_dir / "cc" / crawl
        work.mkdir(parents=True, exist_ok=True)
        last_error = ""
        for attempt in range(args.retries + 1):
            try:
                tenants, ie_domains, report = query_cc(crawl, cc_paths(session, crawl), work)
                for domain in tenants:
                    add_seed(seeds, domain, f"commoncrawl_tenant_{crawl}")
                for domain in ie_domains:
                    add_seed(seeds, domain, f"commoncrawl_ie_{crawl}")
                report["status"] = "success"
                summary["commoncrawl"].append(report)
                successful += 1
                break
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < args.retries:
                    time.sleep(min(60, 3 * (2 ** attempt)))
        else:
            summary["commoncrawl"].append({"crawl": crawl, "status": "failed", "error": last_error})

    seed_path = output_dir / "seeds.csv"
    with seed_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["domain", "sources"])
        writer.writeheader()
        for domain in sorted(seeds):
            writer.writerow({"domain": domain, "sources": ";".join(sorted(seeds[domain]))})
    summary.update({
        "collections_requested": args.collections,
        "collections_successful": successful,
        "unique_candidate_domains": len(seeds),
        "minimum_candidates": args.minimum_candidates,
        "passed": len(seeds) >= args.minimum_candidates,
    })
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)
    return 0 if summary["passed"] else 2


def load_shard(path: Path, shard_count: int, shard_index: int) -> dict[str, str]:
    values: dict[str, str] = {}
    with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        for row in csv.DictReader(handle):
            domain = normalise_domain(row.get("domain", ""))
            if not domain:
                continue
            if int(hashlib.sha256(domain.encode()).hexdigest(), 16) % shard_count == shard_index:
                values[domain] = row.get("sources", "")
    return values


def meta_value(data: dict, key: str):
    direct = data.get(key)
    if direct not in {None, ""}:
        return direct
    shop = data.get("shop")
    return shop.get(key, "") if isinstance(shop, dict) else ""


def valid_meta(data: object) -> bool:
    if not isinstance(data, dict):
        return False
    shop = data.get("shop")
    return bool(
        data.get("id") or data.get("myshopify_domain") or data.get("domain")
        or (isinstance(shop, dict) and (shop.get("id") or shop.get("myshopify_domain") or shop.get("domain")))
    )


async def fetch_meta(session: aiohttp.ClientSession, domain: str, sources: str, timeout: float) -> dict:
    checked = dt.datetime.now(dt.timezone.utc).isoformat()
    status = 0
    final_url = ""
    evidence_url = ""
    error = ""
    data: dict = {}
    for host in (domain, f"www.{domain}"):
        try:
            async with session.get(
                f"https://{host}/meta.json",
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
                headers={"User-Agent": UA, "Accept": "application/json,text/plain;q=0.9,*/*;q=0.4"},
            ) as response:
                status = response.status
                final_url = str(response.url)
                if status != 200:
                    continue
                raw = await response.content.read(1_500_000)
                try:
                    candidate = json.loads(raw.decode(response.charset or "utf-8", errors="replace"))
                except (ValueError, UnicodeError):
                    continue
                if valid_meta(candidate):
                    data = candidate
                    evidence_url = final_url
                    break
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            error = f"{type(exc).__name__}: {exc}"

    shop_id = str(meta_value(data, "id") or "").strip()
    name = str(meta_value(data, "name") or "").strip()
    raw_country = str(meta_value(data, "country") or meta_value(data, "country_code") or "").strip()
    country_key = re.sub(r"\s+", " ", raw_country).strip().upper()
    country = "IE" if country_key in IRELAND_VALUES else country_key
    primary = normalise_domain(str(meta_value(data, "domain") or ""))
    tenant = normalise_domain(str(meta_value(data, "myshopify_domain") or ""))
    final_domain = normalise_domain(urlparse(final_url).hostname or "") if final_url else domain
    canonical = primary or final_domain or domain
    canonical_key = shop_id or tenant or canonical
    verified = bool(data)
    strict = verified and country == "IE"
    signals = ["shopify_public_meta_json"] if verified else []
    if shop_id:
        signals.append("shopify_shop_id")
    if tenant:
        signals.append("shopify_tenant")
    if strict:
        signals.append("shopify_public_country_IE")
    return {
        "domain": domain,
        "seed_sources": sources,
        "canonical_domain": canonical,
        "canonical_key": canonical_key,
        "shopify_verified": str(verified).lower(),
        "ireland_strict": str(strict).lower(),
        "shop_id": shop_id,
        "shop_name": name,
        "shop_country": country,
        "shop_country_raw": raw_country,
        "shop_city": str(meta_value(data, "city") or "").strip(),
        "shop_province": str(meta_value(data, "province") or meta_value(data, "province_code") or "").strip(),
        "shop_currency": str(meta_value(data, "currency") or "").strip().upper(),
        "primary_domain": primary,
        "myshopify_domain": tenant,
        "http_status": status,
        "final_url": final_url,
        "redirect_alias": str(bool(final_domain and final_domain != domain)).lower(),
        "signals": ";".join(signals),
        "evidence_urls": evidence_url,
        "checked_at_utc": checked,
        "error": error,
    }


VERIFY_FIELDS = [
    "domain", "seed_sources", "canonical_domain", "canonical_key", "shopify_verified", "ireland_strict",
    "shop_id", "shop_name", "shop_country", "shop_country_raw", "shop_city", "shop_province",
    "shop_currency", "primary_domain", "myshopify_domain", "http_status", "final_url", "redirect_alias",
    "signals", "evidence_urls", "checked_at_utc", "error",
]


async def verify_async(args: argparse.Namespace) -> int:
    seeds = load_shard(Path(args.input), args.shard_count, args.shard_index)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    strict_path = output_dir / "strict.csv"
    live_path = output_dir / "live.csv"
    strict_rows: list[dict] = []
    live_rows: list[dict] = []
    done = 0
    connector = aiohttp.TCPConnector(limit=max(200, args.concurrency * 2), ttl_dns_cache=600, ssl=False)
    semaphore = asyncio.Semaphore(args.concurrency)
    lock = asyncio.Lock()

    async with aiohttp.ClientSession(connector=connector) as session:
        async def worker(domain: str, sources: str) -> None:
            nonlocal done
            async with semaphore:
                row = await fetch_meta(session, domain, sources, args.timeout)
            async with lock:
                done += 1
                if row["shopify_verified"] == "true":
                    live_rows.append(row)
                if row["ireland_strict"] == "true":
                    strict_rows.append(row)
                if done % 1000 == 0:
                    print(f"SHARD {args.shard_index}: {done}/{len(seeds)} live={len(live_rows)} strict={len(strict_rows)}", flush=True)

        await asyncio.gather(*(worker(domain, sources) for domain, sources in seeds.items()))

    def write(path: Path, rows: list[dict]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=VERIFY_FIELDS)
            writer.writeheader()
            writer.writerows(sorted(rows, key=lambda row: row["domain"]))
    write(strict_path, strict_rows)
    write(live_path, live_rows)
    summary = {
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "seed_count": len(seeds),
        "checked": done,
        "live_shopify": len(live_rows),
        "strict_country_IE": len(strict_rows),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)
    return 0


def audit(args: argparse.Namespace) -> int:
    root = Path(args.root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(root.rglob("strict.csv"))
    if not files:
        raise RuntimeError(f"no strict.csv files under {root}")
    best: dict[str, dict] = {}
    input_rows = 0
    for path in files:
        with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
            for row in csv.DictReader(handle):
                input_rows += 1
                if row.get("shopify_verified", "").casefold() != "true" or row.get("ireland_strict", "").casefold() != "true":
                    continue
                if row.get("shop_country", "").upper() != "IE" or not row.get("evidence_urls", "").strip():
                    continue
                identity = (row.get("shop_id") or row.get("myshopify_domain") or row.get("canonical_key") or row.get("canonical_domain") or row.get("domain") or "").strip().casefold()
                if not identity:
                    continue
                prior = best.get(identity)
                richness = sum(bool((row.get(field) or "").strip()) for field in VERIFY_FIELDS)
                prior_richness = sum(bool((prior.get(field) or "").strip()) for field in VERIFY_FIELDS) if prior else -1
                if richness > prior_richness:
                    best[identity] = row

    accepted_path = output_dir / "verified_irish_shopify_stores.csv"
    with accepted_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=VERIFY_FIELDS)
        writer.writeheader()
        for identity in sorted(best):
            writer.writerow({field: best[identity].get(field, "") for field in VERIFY_FIELDS})
    report = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "shard_files": len(files),
        "input_strict_rows": input_rows,
        "unique_verified_irish_shopify": len(best),
        "minimum_required": args.minimum,
        "publication_gate_passed": len(best) >= args.minimum,
        "shortfall": max(0, args.minimum - len(best)),
    }
    (output_dir / "acceptance_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)
    return 0 if report["publication_gate_passed"] else 2


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    build_parser = sub.add_parser("build")
    build_parser.add_argument("--output-dir", required=True)
    build_parser.add_argument("--collections", type=int, default=12)
    build_parser.add_argument("--retries", type=int, default=3)
    build_parser.add_argument("--minimum-sheet", type=int, default=450_000)
    build_parser.add_argument("--minimum-ie-list", type=int, default=100_000)
    build_parser.add_argument("--minimum-candidates", type=int, default=450_000)

    verify_parser = sub.add_parser("verify")
    verify_parser.add_argument("--input", required=True)
    verify_parser.add_argument("--output-dir", required=True)
    verify_parser.add_argument("--shard-count", type=int, required=True)
    verify_parser.add_argument("--shard-index", type=int, required=True)
    verify_parser.add_argument("--concurrency", type=int, default=300)
    verify_parser.add_argument("--timeout", type=float, default=10.0)

    audit_parser = sub.add_parser("audit")
    audit_parser.add_argument("--root", required=True)
    audit_parser.add_argument("--output-dir", required=True)
    audit_parser.add_argument("--minimum", type=int, default=10_923)

    args = parser.parse_args()
    if args.command == "build":
        return build(args)
    if args.command == "verify":
        if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
            raise SystemExit("invalid shard configuration")
        return asyncio.run(verify_async(args))
    return audit(args)


if __name__ == "__main__":
    raise SystemExit(main())
