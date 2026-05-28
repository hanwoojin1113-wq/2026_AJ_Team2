"""
Kinolights OTT/theater direct-link crawler for CineMatch.

The crawler reads movie candidates from CSV, optionally finds a Kinolights
title page through public search pages, extracts direct watch links, and writes
a cache CSV that can be imported through /admin/ott-links/import-csv.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urljoin, urlparse

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


DEFAULT_INPUT = Path("scripts/kinolights_titles.csv")
DEFAULT_SAMPLE_INPUT = Path("scripts/kinolights_titles.sample.csv")
DEFAULT_OUTPUT = Path("output/kinolights_ott_links.csv")
KINOLIGHTS_BASE = "https://m.kinolights.com"

TERMINAL_STATUSES = {"SUCCESS", "NO_TITLE", "NO_LINK"}

NOISE_DOMAINS = {
    "youtube.com",
    "www.youtube.com",
    "youtube-nocookie.com",
    "www.youtube-nocookie.com",
    "youtu.be",
    "facebook.com",
    "www.facebook.com",
    "instagram.com",
    "www.instagram.com",
    "twitter.com",
    "x.com",
}

KINOLIGHTS_DOMAINS = {
    "kinolights.com",
    "www.kinolights.com",
    "m.kinolights.com",
    "click.kinolights.com",
}

PROVIDER_ALIASES: dict[str, tuple[str, ...]] = {
    "TVING": ("tving", "티빙"),
    "Wavve": ("wavve", "wave", "웨이브"),
    "Watcha": ("watcha", "왓챠"),
    "Coupang Play": ("coupang play", "coupangplay", "coupang", "쿠팡플레이", "쿠팡"),
    "Netflix": ("netflix", "넷플릭스"),
    "Disney+": ("disney+", "disney plus", "disneyplus", "disney", "디즈니"),
    "Apple TV": ("apple tv+", "apple tv", "tv.apple", "apple.com", "apple", "애플"),
    "Google Play": ("google play", "play.google", "google", "구글"),
    "Naver SeriesOn": ("serieson", "series on", "naver", "네이버", "시리즈온"),
    "Prime Video": ("prime video", "primevideo", "amazon", "아마존"),
    "Laftel": ("laftel", "라프텔"),
    "CGV": ("cgv", "씨지브이"),
    "Lotte Cinema": ("lotte cinema", "lottecinema", "롯데시네마", "롯데 시네마"),
    "Megabox": ("megabox", "메가박스"),
}


@dataclass(frozen=True)
class InputTitle:
    movie_id: str
    movie_cd: str
    title: str
    kinolights_url: str


@dataclass
class OttLink:
    movie_id: str
    movie_cd: str
    title: str
    kinolights_url: str
    status: str
    provider: str = ""
    watch_url: str = ""
    raw_url: str = ""
    raw_text: str = ""
    source_method: str = ""
    is_external_direct: bool = False
    error: str = ""


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.lower().replace("\u00a0", " ").split())


def compact_text(value: str | None) -> str:
    return re.sub(r"[\W_]+", "", normalize_text(value), flags=re.UNICODE)


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def is_http_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        return urlparse(url).scheme in {"http", "https"}
    except Exception:
        return False


def is_kinolights_url(url: str | None) -> bool:
    return domain_of(url or "") in KINOLIGHTS_DOMAINS


def is_noise_url(url: str | None) -> bool:
    return domain_of(url or "") in NOISE_DOMAINS


def normalize_kinolights_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("/"):
        return urljoin(KINOLIGHTS_BASE, url)
    return url


def extract_redirect_target(url: str | None) -> str:
    if not is_http_url(url):
        return ""
    parsed = urlparse(url or "")
    if parsed.netloc.lower() != "click.kinolights.com":
        return ""

    target = parse_qs(parsed.query).get("url", [""])[0]
    return target if is_http_url(target) else ""


def direct_watch_url(raw_url: str | None) -> str:
    redirect_target = extract_redirect_target(raw_url)
    if redirect_target and not is_noise_url(redirect_target) and not is_kinolights_url(redirect_target):
        return redirect_target
    if is_http_url(raw_url) and not is_noise_url(raw_url) and not is_kinolights_url(raw_url):
        return raw_url or ""
    return ""


def detect_provider(*parts: str | None) -> str:
    haystack = normalize_text(" ".join(part for part in parts if part))
    if not haystack:
        return ""

    compact = haystack.replace(" ", "")
    for provider, aliases in PROVIDER_ALIASES.items():
        for alias in aliases:
            needle = normalize_text(alias)
            if needle and (needle in haystack or needle.replace(" ", "") in compact):
                return provider
    return ""


def load_input(path: Path) -> list[InputTitle]:
    if not path.exists() and path == DEFAULT_INPUT and DEFAULT_SAMPLE_INPUT.exists():
        print(f"[WARN] {DEFAULT_INPUT} not found. Using sample input: {DEFAULT_SAMPLE_INPUT}")
        path = DEFAULT_SAMPLE_INPUT

    if not path.exists():
        raise SystemExit(f"Input CSV not found: {path}")

    rows: list[InputTitle] = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "title" not in set(reader.fieldnames):
            raise SystemExit("Input CSV must contain at least a title column.")

        for row in reader:
            title = (row.get("title") or "").strip()
            if not title:
                continue
            rows.append(
                InputTitle(
                    movie_id=(row.get("movie_id") or "").strip(),
                    movie_cd=(row.get("movie_cd") or "").strip(),
                    title=title,
                    kinolights_url=(row.get("kinolights_url") or "").strip(),
                )
            )

    return rows


def load_completed_from_output(path: Path) -> set[str]:
    if not path.exists():
        return set()

    completed: set[str] = set()
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = (row.get("status") or "").strip().upper()
            if status not in TERMINAL_STATUSES:
                continue
            key = row_key(
                (row.get("movie_id") or "").strip(),
                (row.get("title") or "").strip(),
            )
            if key:
                completed.add(key)
    return completed


def row_key(movie_id: str, title: str) -> str:
    if movie_id:
        return f"id:{movie_id}"
    if title:
        return f"title:{compact_text(title)}"
    return ""


def dedupe_links(links: list[OttLink]) -> list[OttLink]:
    result: list[OttLink] = []
    seen: set[tuple[str, str, str, str]] = set()
    for link in links:
        key = (row_key(link.movie_id, link.title), link.provider, link.watch_url or link.raw_url, link.status)
        if key in seen:
            continue
        seen.add(key)
        result.append(link)
    return result


async def prepare_page(page: Any, url: str, timeout_ms: int) -> None:
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    try:
        await page.wait_for_load_state("networkidle", timeout=10_000)
    except Exception:
        pass

    for _ in range(4):
        await page.mouse.wheel(0, 1200)
        await page.wait_for_timeout(450)


async def find_kinolights_title_url(context: Any, row: InputTitle, timeout_ms: int) -> str:
    if row.kinolights_url:
        return normalize_kinolights_url(row.kinolights_url)

    search_urls = [
        f"{KINOLIGHTS_BASE}/search?keyword={quote(row.title)}",
        f"{KINOLIGHTS_BASE}/search?q={quote(row.title)}",
        f"{KINOLIGHTS_BASE}/search?search={quote(row.title)}",
    ]

    best_url = ""
    page = await context.new_page()
    try:
        for search_url in search_urls:
            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    await page.wait_for_load_state("networkidle", timeout=8_000)
                except Exception:
                    pass
                await page.wait_for_timeout(800)
            except Exception:
                continue

            candidates = await page.evaluate(
                """
                () => Array.from(document.querySelectorAll('a[href*="/title/"]')).map((node) => {
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    let cur = node;
                    const chunks = [];
                    for (let i = 0; cur && i < 5; i += 1, cur = cur.parentElement) {
                        chunks.push(normalize(cur.innerText || ''));
                    }
                    return {
                        href: node.href || node.getAttribute('href') || '',
                        text: normalize(chunks.join(' | '))
                    };
                })
                """
            )
            best_url = choose_best_title_url(row.title, candidates)
            if best_url:
                return best_url
    finally:
        await page.close()

    return ""


def choose_best_title_url(title: str, candidates: list[dict[str, str]]) -> str:
    target = compact_text(title)
    if not candidates:
        return ""

    unique: dict[str, str] = {}
    for candidate in candidates:
        href = normalize_kinolights_url(candidate.get("href", ""))
        if "/title/" not in href:
            continue
        unique.setdefault(href.split("?")[0], candidate.get("text", ""))

    if not unique:
        return ""

    scored: list[tuple[int, str]] = []
    for href, text in unique.items():
        compact = compact_text(text)
        score = 0
        if target and target in compact:
            score += 100
        if compact and compact in target:
            score += 40
        score -= len(scored)
        scored.append((score, href))

    scored.sort(reverse=True)
    return scored[0][1] if scored[0][0] > 0 else next(iter(unique.keys()))


async def extract_anchor_links(page: Any, row: InputTitle, kinolights_url: str) -> list[OttLink]:
    items = await page.evaluate(
        """
        () => {
            const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
            const ancestorText = (node) => {
                const chunks = [];
                let cur = node;
                for (let i = 0; cur && i < 5; i += 1, cur = cur.parentElement) {
                    chunks.push(normalize(cur.innerText || ''));
                }
                return chunks.join(' | ');
            };

            return Array.from(document.querySelectorAll('a[href]')).map((node) => {
                const imgAlt = Array.from(node.querySelectorAll('img'))
                    .map((img) => img.getAttribute('alt') || '')
                    .filter(Boolean)
                    .join(' ');
                return {
                    text: normalize(node.innerText || ''),
                    aria: normalize(node.getAttribute('aria-label') || ''),
                    title: normalize(node.getAttribute('title') || ''),
                    imgAlt: normalize(imgAlt),
                    href: node.href || node.getAttribute('href') || '',
                    ancestorText: ancestorText(node)
                };
            });
        }
        """
    )

    links: list[OttLink] = []
    for item in items:
        href = item.get("href", "")
        if not is_http_url(href) or is_noise_url(href):
            continue

        target_url = direct_watch_url(href) or href
        if is_noise_url(target_url):
            continue

        label_parts = [
            item.get("text", ""),
            item.get("aria", ""),
            item.get("title", ""),
            item.get("imgAlt", ""),
        ]
        provider = detect_provider(target_url, href) or detect_provider(*label_parts)
        if not provider:
            continue

        watch_url = direct_watch_url(href)
        raw_text = next((part for part in [*label_parts, item.get("ancestorText", "")] if part), "")
        links.append(
            OttLink(
                movie_id=row.movie_id,
                movie_cd=row.movie_cd,
                title=row.title,
                kinolights_url=kinolights_url,
                status="SUCCESS" if watch_url else "NO_LINK",
                provider=provider,
                watch_url=watch_url,
                raw_url=href,
                raw_text=raw_text,
                source_method="anchor",
                is_external_direct=bool(watch_url),
            )
        )

    return links


async def mark_click_candidates(page: Any) -> list[dict[str, Any]]:
    return await page.evaluate(
        """
        () => {
            const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
            const providerPattern = /(tving|티빙|wavve|wave|웨이브|watcha|왓챠|coupang|쿠팡|netflix|넷플릭스|disney|디즈니|apple|애플|google|구글|naver|네이버|serieson|시리즈온|prime|amazon|laftel|라프텔|cgv|씨지브이|lotte|롯데|megabox|메가박스)/i;

            const candidates = [];
            const nodes = Array.from(document.querySelectorAll('a[href], button, [role="button"], [onclick]'));
            nodes.forEach((node, index) => {
                const text = normalize([
                    node.innerText || '',
                    node.getAttribute('aria-label') || '',
                    node.getAttribute('title') || '',
                    Array.from(node.querySelectorAll('img')).map((img) => img.getAttribute('alt') || '').join(' ')
                ].join(' '));
                const href = node.href || node.getAttribute('href') || '';
                if (!providerPattern.test(`${text} ${href}`)) return;
                const attr = `cm-ott-${index}`;
                node.setAttribute('data-cm-ott-crawl-index', attr);
                candidates.push({ index: attr, text, href });
            });
            return candidates;
        }
        """
    )


async def click_provider_candidates(
    page: Any,
    row: InputTitle,
    kinolights_url: str,
    timeout_ms: int,
    max_clicks: int,
) -> list[OttLink]:
    candidates = await mark_click_candidates(page)
    links: list[OttLink] = []
    clicked = 0

    for candidate in candidates:
        if clicked >= max_clicks:
            break

        href = candidate.get("href", "")
        target_url = direct_watch_url(href) or href
        provider = detect_provider(target_url, href) or detect_provider(candidate.get("text", ""))
        if not provider:
            continue
        if direct_watch_url(href):
            continue

        locator = page.locator(f'[data-cm-ott-crawl-index="{candidate["index"]}"]').first
        try:
            if await locator.count() == 0:
                continue
        except Exception:
            continue

        old_url = page.url
        clicked += 1
        captured_url = ""
        source_method = "click"

        try:
            async with page.expect_popup(timeout=3_000) as popup_info:
                await locator.click(timeout=3_000, force=True)
            popup = await popup_info.value
            try:
                await popup.wait_for_load_state("domcontentloaded", timeout=8_000)
            except Exception:
                pass
            captured_url = popup.url
            source_method = "click_popup"
            await popup.close()
        except Exception:
            await page.wait_for_timeout(1_200)
            if page.url != old_url:
                captured_url = page.url
                source_method = "click_navigation"
                try:
                    await prepare_page(page, kinolights_url, timeout_ms)
                    await mark_click_candidates(page)
                except Exception:
                    pass

        target_url = direct_watch_url(captured_url) or captured_url
        if not is_http_url(target_url) or is_noise_url(target_url):
            continue

        watch_url = direct_watch_url(captured_url)
        provider = detect_provider(target_url, captured_url) or provider
        links.append(
            OttLink(
                movie_id=row.movie_id,
                movie_cd=row.movie_cd,
                title=row.title,
                kinolights_url=kinolights_url,
                status="SUCCESS" if watch_url else "NO_LINK",
                provider=provider,
                watch_url=watch_url,
                raw_url=captured_url,
                raw_text=candidate.get("text", ""),
                source_method=source_method,
                is_external_direct=bool(watch_url),
            )
        )

    return links


async def crawl_one(
    browser_context: Any,
    row: InputTitle,
    timeout_ms: int,
    search_timeout_ms: int,
    max_clicks: int,
    auto_search: bool,
) -> list[OttLink]:
    page = await browser_context.new_page()
    try:
        kinolights_url = normalize_kinolights_url(row.kinolights_url)
        if not kinolights_url and auto_search:
            kinolights_url = await find_kinolights_title_url(browser_context, row, search_timeout_ms)

        if not kinolights_url:
            print(f"[NO_TITLE] {row.title}")
            return [
                OttLink(
                    movie_id=row.movie_id,
                    movie_cd=row.movie_cd,
                    title=row.title,
                    kinolights_url="",
                    status="NO_TITLE",
                    error="Kinolights title page not found",
                )
            ]

        print(f"[START] {row.title} - {kinolights_url}")
        await prepare_page(page, kinolights_url, timeout_ms)
        links = await extract_anchor_links(page, row, kinolights_url)
        links.extend(await click_provider_candidates(page, row, kinolights_url, timeout_ms, max_clicks))
        links = dedupe_links(links)
        direct_count = sum(1 for link in links if link.is_external_direct and link.watch_url)
        if direct_count == 0:
            print(f"[NO_LINK] {row.title}")
            return [
                OttLink(
                    movie_id=row.movie_id,
                    movie_cd=row.movie_cd,
                    title=row.title,
                    kinolights_url=kinolights_url,
                    status="NO_LINK",
                    error="No direct watch links found",
                )
            ]

        for link in links:
            if link.watch_url and link.is_external_direct:
                link.status = "SUCCESS"
        links = [link for link in links if link.watch_url and link.is_external_direct]
        print(f"[DONE] {row.title} - {len(links)} candidates, {direct_count} direct URLs")
        return links
    except Exception as exc:
        print(f"[FAILED] {row.title}: {exc}")
        return [
            OttLink(
                movie_id=row.movie_id,
                movie_cd=row.movie_cd,
                title=row.title,
                kinolights_url=row.kinolights_url,
                status="FAILED",
                error=str(exc),
            )
        ]
    finally:
        await page.close()


def write_csv(path: Path, rows: list[OttLink], append: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "movie_id",
        "movie_cd",
        "title",
        "kinolights_url",
        "status",
        "provider",
        "watch_url",
        "raw_url",
        "raw_text",
        "source_method",
        "is_external_direct",
        "error",
        "crawled_at",
    ]
    crawled_at = datetime.now(timezone.utc).isoformat()
    mode = "a" if append and path.exists() else "w"

    with path.open(mode, newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "movie_id": row.movie_id,
                    "movie_cd": row.movie_cd,
                    "title": row.title,
                    "kinolights_url": row.kinolights_url,
                    "status": row.status,
                    "provider": row.provider,
                    "watch_url": row.watch_url,
                    "raw_url": row.raw_url,
                    "raw_text": row.raw_text,
                    "source_method": row.source_method,
                    "is_external_direct": str(row.is_external_direct).lower(),
                    "error": row.error,
                    "crawled_at": crawled_at,
                }
            )


def print_summary(input_count: int, rows: list[OttLink], output: Path) -> None:
    direct_rows = [row for row in rows if row.watch_url and row.is_external_direct]
    success_titles = {row_key(row.movie_id, row.title) for row in direct_rows}
    no_title = {row_key(row.movie_id, row.title) for row in rows if row.status == "NO_TITLE"}
    no_link = {row_key(row.movie_id, row.title) for row in rows if row.status == "NO_LINK"}
    failed = {row_key(row.movie_id, row.title) for row in rows if row.status == "FAILED"}
    provider_counts: dict[str, int] = {}
    for row in direct_rows:
        provider_counts[row.provider] = provider_counts.get(row.provider, 0) + 1

    print("")
    print("Kinolights OTT crawl summary")
    print(f"- input titles: {input_count}")
    print(f"- titles with direct URL: {len(success_titles)}")
    print(f"- no Kinolights title: {len(no_title)}")
    print(f"- no watch links: {len(no_link)}")
    print(f"- failed: {len(failed)}")
    print(f"- direct URL count: {len(direct_rows)}")
    for provider in PROVIDER_ALIASES:
        print(f"- {provider}: {provider_counts.get(provider, 0)}")
    print(f"- saved: {output}")


async def run(args: argparse.Namespace) -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise SystemExit(
            "Playwright is not installed. Run: python -m pip install playwright && python -m playwright install chromium"
        ) from exc

    input_rows = load_input(Path(args.input))
    output_path = Path(args.output)
    completed = load_completed_from_output(output_path) if args.resume else set()
    if completed:
        input_rows = [row for row in input_rows if row_key(row.movie_id, row.title) not in completed]

    if args.max_items > 0:
        input_rows = input_rows[: args.max_items]

    all_links: list[OttLink] = []
    user_agent = (
        f"Mozilla/5.0 CineMatchOttCrawler/0.2 (public-page cache; contact: {args.contact})"
        if args.contact
        else "Mozilla/5.0 CineMatchOttCrawler/0.2 (public-page cache)"
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.headful)
        context = await browser.new_context(
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            user_agent=user_agent,
            viewport={"width": 390, "height": 1200},
        )

        for index, row in enumerate(input_rows, start=1):
            print(f"[{index}/{len(input_rows)}]")
            links = await crawl_one(
                context,
                row,
                timeout_ms=args.timeout_ms,
                search_timeout_ms=args.search_timeout_ms,
                max_clicks=args.max_clicks,
                auto_search=not args.no_auto_search,
            )
            all_links.extend(links)
            write_csv(output_path, links, append=args.resume or index > 1)

            if index < len(input_rows):
                delay = random.uniform(args.delay_min, args.delay_max)
                await asyncio.sleep(delay)

        await browser.close()

    print_summary(len(input_rows), all_links, output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl Kinolights title pages and cache OTT/theater direct-link candidates."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input CSV with title and optional movie_id/movie_cd/kinolights_url")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path")
    parser.add_argument("--max-items", type=int, default=100, help="Maximum titles to crawl; 0 means no limit")
    parser.add_argument("--delay-min", type=float, default=3.0, help="Minimum delay between pages")
    parser.add_argument("--delay-max", type=float, default=7.0, help="Maximum delay between pages")
    parser.add_argument("--timeout-ms", type=int, default=60_000, help="Page timeout in milliseconds")
    parser.add_argument("--search-timeout-ms", type=int, default=12_000, help="Kinolights search page timeout in milliseconds")
    parser.add_argument("--max-clicks", type=int, default=8, help="Max provider-looking button clicks per title")
    parser.add_argument("--headful", action="store_true", help="Run Chromium visibly for debugging")
    parser.add_argument("--contact", default="", help="Contact string included in the user-agent")
    parser.add_argument("--resume", action="store_true", help="Append to output and skip terminal rows already present there")
    parser.add_argument("--no-auto-search", action="store_true", help="Do not search Kinolights when kinolights_url is blank")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
