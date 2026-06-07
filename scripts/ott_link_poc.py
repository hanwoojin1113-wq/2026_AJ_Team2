from __future__ import annotations

import csv
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


DEFAULT_STREAMING_AVAILABILITY_HOST = "api.movieofthenight.com"
DEFAULT_RAPIDAPI_HOST = "streaming-availability.p.rapidapi.com"
TMDB_BASE_URL = "https://api.themoviedb.org/3"
WATCHMODE_BASE_URL = "https://api.watchmode.com/v1"
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_SLEEP_SECONDS = 0.20
OUTPUT_PATH = Path("output/ott_link_poc_results.csv")
LOCAL_PROPERTIES_PATH = Path("src/main/resources/application-local.properties")

TEST_MOVIES = [
    "파묘",
    "서울의 봄",
    "범죄도시4",
    "범죄도시3",
    "범죄도시2",
    "기생충",
    "헤어질 결심",
    "부산행",
    "올드보이",
    "극한직업",
    "콘크리트 유토피아",
    "밀수",
    "한산: 용의 출현",
    "모가디슈",
    "헌트",
    "곡성",
    "아가씨",
    "마녀",
    "신세계",
    "변호인",
    "7번방의 선물",
    "명량",
    "암살",
    "내부자들",
    "택시운전사",
    "인터스텔라",
    "라라랜드",
    "듄",
    "스파이더맨: 노 웨이 홈",
    "어벤져스: 엔드게임",
]

PROVIDER_ORDER = [
    "tving",
    "wavve",
    "watcha",
    "coupang_play",
    "netflix",
    "disney_plus",
    "apple_tv",
    "google_play",
    "naver_serieson",
]

PROVIDER_CONFIG = {
    "tving": {
        "display": "TVING",
        "aliases": ["tving", "티빙"],
        "column": "tving_url",
        "is_core": True,
    },
    "wavve": {
        "display": "Wavve",
        "aliases": ["wavve", "wave", "웨이브"],
        "column": "wavve_url",
        "is_core": True,
    },
    "watcha": {
        "display": "Watcha",
        "aliases": ["watcha", "왓챠"],
        "column": "watcha_url",
        "is_core": True,
    },
    "coupang_play": {
        "display": "Coupang Play",
        "aliases": ["coupang play", "coupang", "쿠팡플레이", "쿠팡"],
        "column": "coupang_play_url",
        "is_core": True,
    },
    "netflix": {
        "display": "Netflix",
        "aliases": ["netflix", "넷플릭스"],
        "column": "netflix_url",
        "is_core": False,
    },
    "disney_plus": {
        "display": "Disney+",
        "aliases": ["disney+", "disney plus", "disney", "디즈니"],
        "column": "disney_plus_url",
        "is_core": False,
    },
    "apple_tv": {
        "display": "Apple TV",
        "aliases": ["apple tv+", "apple tv", "apple"],
        "column": "apple_tv_url",
        "is_core": False,
    },
    "google_play": {
        "display": "Google Play",
        "aliases": ["google play", "google", "구글"],
        "column": "google_play_url",
        "is_core": False,
    },
    "naver_serieson": {
        "display": "Naver SeriesOn",
        "aliases": ["naver", "serieson", "series on", "네이버", "시리즈온"],
        "column": "naver_serieson_url",
        "is_core": False,
    },
}

CSV_COLUMNS = [
    "input_title",
    "tmdb_id",
    "tmdb_title",
    "tmdb_original_title",
    "tmdb_release_date",
    "tmdb_matched",
    "api_name",
    "api_matched",
    "all_providers",
    "korean_core_providers",
    "korean_core_direct_count",
    "tving_url",
    "wavve_url",
    "watcha_url",
    "coupang_play_url",
    "netflix_url",
    "disney_plus_url",
    "apple_tv_url",
    "google_play_url",
    "naver_serieson_url",
    "other_urls",
    "error",
]


@dataclass
class TmdbMatch:
    input_title: str
    tmdb_id: str
    tmdb_title: str
    tmdb_original_title: str
    tmdb_release_date: str
    tmdb_matched: bool
    error: str = ""


@dataclass
class ApiResult:
    api_name: str
    api_matched: bool
    provider_urls: dict[str, str]
    all_providers: list[str]
    other_urls: list[str]
    error: str = ""


def main() -> int:
    print("OTT link PoC started.")

    local_properties = load_local_properties(LOCAL_PROPERTIES_PATH)
    tmdb_token = read_secret("TMDB_TOKEN", local_properties, "tmdb.token")
    if not tmdb_token:
        print(
            "ERROR: TMDB_TOKEN is required. Set it as an environment variable or in application-local.properties.",
            file=sys.stderr,
        )
        return 1

    streaming_api_key = read_secret(
        "STREAMING_AVAILABILITY_API_KEY",
        local_properties,
        "streaming.availability.api-key",
    )
    watchmode_api_key = read_secret("WATCHMODE_API_KEY", local_properties, "watchmode.api-key")

    clients: list[tuple[str, Any]] = []
    if streaming_api_key:
        clients.append(("STREAMING_AVAILABILITY", StreamingAvailabilityClient(streaming_api_key)))
    else:
        print("Skipping Streaming Availability API: key not provided in env or application-local.properties.")

    if watchmode_api_key:
        clients.append(("WATCHMODE", WatchmodeClient(watchmode_api_key)))
    else:
        print("Skipping Watchmode API: key not provided in env or application-local.properties.")

    if not clients:
        print("No OTT API keys available. Nothing to test.")
        return 1

    session = requests.Session()
    results: list[dict[str, str]] = []
    tmdb_matches: dict[str, TmdbMatch] = {}

    for input_title in TEST_MOVIES:
        tmdb_match = fetch_tmdb_match(session, tmdb_token, input_title)
        tmdb_matches[input_title] = tmdb_match

        for api_name, client in clients:
            api_result = run_api_lookup(session, client, api_name, tmdb_match)
            results.append(build_csv_row(tmdb_match, api_result))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_csv(results, OUTPUT_PATH)
    print(f"CSV written to {OUTPUT_PATH}")
    print_summary(results, len(TEST_MOVIES), tmdb_matches)
    return 0


def fetch_tmdb_match(session: requests.Session, tmdb_token: str, input_title: str) -> TmdbMatch:
    headers = {"Authorization": f"Bearer {tmdb_token}", "Accept": "application/json"}
    params = {
        "query": input_title,
        "language": "ko-KR",
        "region": "KR",
        "include_adult": "false",
    }
    data, error = safe_get_json(
        session,
        f"{TMDB_BASE_URL}/search/movie",
        headers=headers,
        params=params,
    )
    pause()

    if error:
        return TmdbMatch(input_title, "", "", "", "", False, error)

    results = data.get("results") if isinstance(data, dict) else None
    if not isinstance(results, list) or not results:
        return TmdbMatch(input_title, "", "", "", "", False, "TMDB search returned no results")

    best = choose_tmdb_result(input_title, results)
    if not isinstance(best, dict):
        return TmdbMatch(input_title, "", "", "", "", False, "TMDB search result parse failed")

    return TmdbMatch(
        input_title=input_title,
        tmdb_id=str(best.get("id", "") or ""),
        tmdb_title=str(best.get("title", "") or ""),
        tmdb_original_title=str(best.get("original_title", "") or ""),
        tmdb_release_date=str(best.get("release_date", "") or ""),
        tmdb_matched=bool(best.get("id")),
        error="" if best.get("id") else "TMDB search returned unusable result",
    )


def choose_tmdb_result(input_title: str, results: list[Any]) -> dict[str, Any] | None:
    target = normalize_text(input_title)
    best_result = None
    best_score = -1

    for index, item in enumerate(results[:10]):
        if not isinstance(item, dict):
            continue
        score = 0
        title = str(item.get("title", "") or "")
        original_title = str(item.get("original_title", "") or "")
        norm_title = normalize_text(title)
        norm_original = normalize_text(original_title)

        if norm_title == target:
            score += 120
        elif target and target in norm_title:
            score += 90

        if norm_original == target:
            score += 100
        elif target and target in norm_original:
            score += 70

        popularity = item.get("popularity")
        if isinstance(popularity, (int, float)):
            score += min(int(popularity), 20)

        score -= index

        if score > best_score:
            best_score = score
            best_result = item

    return best_result


def run_api_lookup(session: requests.Session, client: Any, api_name: str, tmdb_match: TmdbMatch) -> ApiResult:
    if not tmdb_match.tmdb_matched or not tmdb_match.tmdb_id:
        return ApiResult(
            api_name=api_name,
            api_matched=False,
            provider_urls={},
            all_providers=[],
            other_urls=[],
            error=f"TMDB not matched; skipped {api_name}",
        )

    try:
        return client.lookup_movie(session, tmdb_match)
    except Exception as exc:  # pragma: no cover - defensive for third-party API drift
        return ApiResult(
            api_name=api_name,
            api_matched=False,
            provider_urls={},
            all_providers=[],
            other_urls=[],
            error=f"{api_name} exception: {exc}",
        )


class StreamingAvailabilityClient:
    def __init__(self, api_key: str) -> None:
        local_properties = load_local_properties(LOCAL_PROPERTIES_PATH)
        rapid_host = read_secret("RAPIDAPI_HOST", local_properties, "rapidapi.host")
        self.api_name = "STREAMING_AVAILABILITY"
        self.api_key = api_key
        self.use_rapidapi = bool(rapid_host)
        self.host = rapid_host or DEFAULT_STREAMING_AVAILABILITY_HOST
        self.base_url = (
            f"https://{self.host}"
            if self.use_rapidapi
            else f"https://{DEFAULT_STREAMING_AVAILABILITY_HOST}/v4"
        )

    def lookup_movie(self, session: requests.Session, tmdb_match: TmdbMatch) -> ApiResult:
        headers = self._headers()
        tmdb_path = f"movie/{tmdb_match.tmdb_id}"
        params = {"country": "kr", "output_language": "en"}
        data, error = safe_get_json(
            session,
            f"{self.base_url}/shows/{tmdb_path}",
            headers=headers,
            params=params,
        )
        pause()

        if error:
            return ApiResult(self.api_name, False, {}, [], [], error=error)

        options = extract_streaming_availability_options(data)
        provider_urls, all_providers, other_urls = map_provider_links(options)
        api_matched = bool(all_providers or provider_urls)

        return ApiResult(
            api_name=self.api_name,
            api_matched=api_matched,
            provider_urls=provider_urls,
            all_providers=all_providers,
            other_urls=other_urls,
            error="" if api_matched else "No providers found in Streaming Availability response",
        )

    def _headers(self) -> dict[str, str]:
        if self.use_rapidapi:
            return {
                "X-RapidAPI-Key": self.api_key,
                "X-RapidAPI-Host": self.host or DEFAULT_RAPIDAPI_HOST,
                "Accept": "application/json",
            }
        return {"X-API-Key": self.api_key, "Accept": "application/json"}


class WatchmodeClient:
    def __init__(self, api_key: str) -> None:
        self.api_name = "WATCHMODE"
        self.api_key = api_key

    def lookup_movie(self, session: requests.Session, tmdb_match: TmdbMatch) -> ApiResult:
        title_id, search_error = self._find_title_id(session, tmdb_match)
        if not title_id:
            return ApiResult(self.api_name, False, {}, [], [], error=search_error or "Watchmode title lookup failed")

        sources, sources_error = self._fetch_sources(session, title_id)
        if not sources:
            return ApiResult(self.api_name, False, {}, [], [], error=sources_error or "Watchmode returned no sources")

        provider_urls, all_providers, other_urls = map_provider_links(sources)
        api_matched = bool(all_providers or provider_urls)

        return ApiResult(
            api_name=self.api_name,
            api_matched=api_matched,
            provider_urls=provider_urls,
            all_providers=all_providers,
            other_urls=other_urls,
            error="" if api_matched else (sources_error or "No providers found in Watchmode response"),
        )

    def _find_title_id(self, session: requests.Session, tmdb_match: TmdbMatch) -> tuple[str, str]:
        params = {
            "apiKey": self.api_key,
            "search_field": "tmdb_id",
            "search_value": tmdb_match.tmdb_id,
            "types": "movie",
        }
        data, error = safe_get_json(session, f"{WATCHMODE_BASE_URL}/search/", params=params)
        pause()
        title_id = extract_watchmode_title_id(data)
        if title_id:
            return title_id, ""

        title_params = {
            "apiKey": self.api_key,
            "search_field": "name",
            "search_value": tmdb_match.tmdb_title or tmdb_match.input_title,
            "types": "movie",
        }
        data, title_error = safe_get_json(session, f"{WATCHMODE_BASE_URL}/search/", params=title_params)
        pause()
        title_id = extract_watchmode_title_id(data)
        if title_id:
            return title_id, ""

        joined_error = "; ".join(part for part in [error, title_error] if part)
        return "", joined_error or "Watchmode search returned no usable title id"

    def _fetch_sources(self, session: requests.Session, title_id: str) -> tuple[list[dict[str, Any]], str]:
        params = {"apiKey": self.api_key, "regions": "KR"}
        data, error = safe_get_json(session, f"{WATCHMODE_BASE_URL}/title/{title_id}/sources/", params=params)
        pause()
        sources = extract_watchmode_sources(data)
        if sources:
            return sources, ""

        detail_params = {
            "apiKey": self.api_key,
            "append_to_response": "sources",
            "regions": "KR",
        }
        detail_data, detail_error = safe_get_json(
            session,
            f"{WATCHMODE_BASE_URL}/title/{title_id}/details/",
            params=detail_params,
        )
        pause()
        sources = extract_watchmode_sources(detail_data)
        if sources:
            return sources, ""

        joined_error = "; ".join(part for part in [error, detail_error] if part)
        return [], joined_error or "Watchmode sources/details returned no sources"


def build_csv_row(tmdb_match: TmdbMatch, api_result: ApiResult) -> dict[str, str]:
    row = {column: "" for column in CSV_COLUMNS}
    row["input_title"] = tmdb_match.input_title
    row["tmdb_id"] = tmdb_match.tmdb_id
    row["tmdb_title"] = tmdb_match.tmdb_title
    row["tmdb_original_title"] = tmdb_match.tmdb_original_title
    row["tmdb_release_date"] = tmdb_match.tmdb_release_date
    row["tmdb_matched"] = bool_str(tmdb_match.tmdb_matched)
    row["api_name"] = api_result.api_name
    row["api_matched"] = bool_str(api_result.api_matched)
    row["all_providers"] = " | ".join(api_result.all_providers)
    row["other_urls"] = " | ".join(api_result.other_urls)
    row["error"] = "; ".join(part for part in [tmdb_match.error, api_result.error] if part)

    found_core_providers: list[str] = []
    core_direct_count = 0

    for provider_key in PROVIDER_ORDER:
        config = PROVIDER_CONFIG[provider_key]
        url = api_result.provider_urls.get(provider_key, "")
        row[config["column"]] = url
        if provider_key in api_result.provider_urls:
            if config["is_core"]:
                found_core_providers.append(config["display"])
            if url and config["is_core"]:
                core_direct_count += 1

    row["korean_core_providers"] = " | ".join(found_core_providers)
    row["korean_core_direct_count"] = str(core_direct_count)
    return row


def extract_streaming_availability_options(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    options = data.get("streamingOptions")
    if isinstance(options, dict):
        kr_options = options.get("kr") or options.get("KR")
        return flatten_option_bucket(kr_options)
    return flatten_option_bucket(options)


def extract_watchmode_title_id(data: Any) -> str:
    candidates = []

    if isinstance(data, list):
        candidates = data
    elif isinstance(data, dict):
        for key in ("title_results", "results"):
            value = data.get(key)
            if isinstance(value, list):
                candidates = value
                break

    for item in candidates:
        if not isinstance(item, dict):
            continue
        value = item.get("id") or item.get("title_id")
        if value is not None:
            return str(value)
    return ""


def extract_watchmode_sources(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        if isinstance(data.get("sources"), list):
            return [item for item in data["sources"] if isinstance(item, dict)]
        if isinstance(data.get("source_results"), list):
            return [item for item in data["source_results"] if isinstance(item, dict)]
    return []


def flatten_option_bucket(bucket: Any) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    if isinstance(bucket, list):
        return [item for item in bucket if isinstance(item, dict)]
    if isinstance(bucket, dict):
        for value in bucket.values():
            if isinstance(value, list):
                options.extend(item for item in value if isinstance(item, dict))
    return options


def map_provider_links(options: list[dict[str, Any]]) -> tuple[dict[str, str], list[str], list[str]]:
    provider_urls: dict[str, str] = {}
    all_providers: list[str] = []
    other_urls: list[str] = []

    for option in options:
        raw_name = extract_provider_name(option)
        if not raw_name:
            continue

        all_providers.append(raw_name)
        provider_key = match_provider_key(raw_name)
        url = extract_provider_url(option)

        if provider_key:
            # 첫 direct URL을 우선 보존하고, 이후에는 빈 값일 때만 대체한다.
            if provider_key not in provider_urls or (not provider_urls[provider_key] and url):
                provider_urls[provider_key] = url
        elif url:
            other_urls.append(f"{raw_name}={url}")

    return provider_urls, unique(all_providers), unique(other_urls)


def extract_provider_name(option: dict[str, Any]) -> str:
    candidates = []
    service = option.get("service")
    if isinstance(service, dict):
        candidates.extend(
            [
                service.get("name"),
                service.get("id"),
                service.get("serviceName"),
            ]
        )

    candidates.extend(
        [
            option.get("name"),
            option.get("serviceName"),
            option.get("source_name"),
            option.get("source"),
            option.get("provider"),
            option.get("provider_name"),
        ]
    )

    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return ""


def extract_provider_url(option: dict[str, Any]) -> str:
    candidates = [
        option.get("videoLink"),
        option.get("link"),
        option.get("deepLink"),
        option.get("deeplink"),
        option.get("webUrl"),
        option.get("web_url"),
        option.get("url"),
        option.get("ios_url"),
        option.get("android_url"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return ""


def match_provider_key(name: str) -> str:
    normalized = normalize_text(name)
    for provider_key, config in PROVIDER_CONFIG.items():
        for alias in config["aliases"]:
            if normalize_text(alias) in normalized:
                return provider_key
    return ""


def safe_get_json(
    session: requests.Session,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
) -> tuple[Any, str]:
    try:
        response = session.get(
            url,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code >= 400:
            snippet = response.text[:240].replace("\n", " ").strip()
            return None, f"HTTP {response.status_code}: {snippet}"
        return response.json(), ""
    except requests.RequestException as exc:
        return None, f"Request failed: {exc}"
    except ValueError as exc:
        return None, f"JSON parse failed: {exc}"


def load_local_properties(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    properties: dict[str, str] = {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                properties[key.strip()] = value.strip()
    except OSError:
        return {}
    return properties


def read_secret(env_name: str, local_properties: dict[str, str], property_name: str) -> str:
    env_value = os.getenv(env_name, "").strip()
    if env_value:
        return env_value
    return local_properties.get(property_name, "").strip()


def write_csv(rows: list[dict[str, str]], path: Path) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows: list[dict[str, str]], tested_movie_count: int, tmdb_matches: dict[str, TmdbMatch]) -> None:
    tmdb_matched_count = sum(1 for match in tmdb_matches.values() if match.tmdb_matched)
    executed_api_names = unique(row["api_name"] for row in rows)

    for api_name in executed_api_names:
        api_rows = [row for row in rows if row["api_name"] == api_name]
        matched_movies = sum(1 for row in api_rows if row["api_matched"] == "true")
        core_found_movies = sum(1 for row in api_rows if row["korean_core_providers"])
        core_direct_links = sum(int(row["korean_core_direct_count"] or "0") for row in api_rows)

        print()
        print(f"{api_name}:")
        print(f"- tested movies: {tested_movie_count}")
        print(f"- tmdb matched: {tmdb_matched_count}")
        print(f"- api matched movies: {matched_movies}")
        print(f"- Korean core OTT found movies: {core_found_movies}")
        print(f"- Korean core direct links: {core_direct_links}")

        for provider_key in PROVIDER_ORDER:
            config = PROVIDER_CONFIG[provider_key]
            count = sum(1 for row in api_rows if row[config["column"]].strip())
            print(f"- {config['display']}: {count}")


def unique(values: Any) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        stripped = value.strip()
        if not stripped or stripped in seen:
            continue
        seen.add(stripped)
        output.append(stripped)
    return output


def normalize_text(text: str) -> str:
    return (
        (text or "")
        .lower()
        .replace(" ", "")
        .replace(":", "")
        .replace("-", "")
        .replace("_", "")
        .replace("’", "")
        .replace("'", "")
        .replace(".", "")
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
        .strip()
    )


def bool_str(value: bool) -> str:
    return "true" if value else "false"


def pause() -> None:
    time.sleep(REQUEST_SLEEP_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
