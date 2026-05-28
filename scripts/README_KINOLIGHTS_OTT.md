# Kinolights OTT Link Crawler

## 목적

영화 상세 페이지의 `보러가기` 섹션에 사용할 OTT/극장 direct URL을 소규모로 수집해 DB 캐시로 관리한다.

서비스 요청 시마다 크롤링하지 않는다. 공개 키노라이츠 작품 페이지를 낮은 빈도로 확인하고, 결과를 CSV로 저장한 뒤 `/admin/ott-links/import-csv`로 DB에 적재한다.

## 관리 방식

크롤링 결과는 세 가지 상태로 분리해서 관리한다.

- `movie_ott_link`: 실제 표시할 제공처 URL. 영화별 제공처 하나만 저장한다. 같은 제공처가 여러 개면 키노라이츠에서 먼저 나온 URL만 사용한다.
- `movie_ott_crawl_status`: 영화별 크롤링 상태. `SUCCESS`, `NO_TITLE`, `NO_LINK`, `FAILED`를 저장한다.
- `kinolights_title_mapping`: 우리 DB의 movie_id와 키노라이츠 작품 URL 매핑.

중복 방지 기준은 `movie_ott_link` 존재 여부가 아니라 `movie_ott_crawl_status`다. 그래서 URL 1개가 이미 있다는 이유로 같은 영화의 나머지 제공처를 놓치는 문제를 피한다.

## 설치

```powershell
python -m pip install playwright
python -m playwright install chromium
```

## 200개 후보 CSV 만들기

Spring Boot 앱을 실행한 뒤 후보 CSV를 받는다.

```powershell
$base = "http://localhost:8080"
Invoke-WebRequest "$base/admin/ott-links/candidates.csv?limit=200" -OutFile "output/kinolights_candidates_200.csv"
```

이미 `SUCCESS`, `NO_TITLE`, `NO_LINK` 상태인 영화는 후보에서 제외된다. `FAILED`는 재시도 대상이다.

## 크롤링 실행

```powershell
python scripts/kinolights_ott_crawler.py `
  --input output/kinolights_candidates_200.csv `
  --output output/kinolights_ott_links_200.csv `
  --max-items 200 `
  --delay-min 3 `
  --delay-max 7 `
  --max-clicks 0 `
  --resume
```

`kinolights_url`이 비어 있으면 크롤러가 키노라이츠 검색 페이지에서 `/title/{id}` URL을 찾는다. 못 찾으면 `NO_TITLE`로 남긴다.
대량 실행에서는 우선 `--max-clicks 0`으로 anchor/redirect URL만 수집하는 편이 안정적이다. 클릭으로 새 창을 여는 케이스까지 확인해야 할 때만 `--max-clicks 2` 이상으로 다시 보강한다.

## DB 적재

크롤링 CSV를 앱으로 import한다.

```powershell
$base = "http://localhost:8080"
Invoke-RestMethod -Method Post "$base/admin/ott-links/import-csv?path=output/kinolights_ott_links_200.csv"
```

기존 10개 결과를 새 관리 방식으로 옮기려면 다음을 먼저 실행한다.

```powershell
$base = "http://localhost:8080"
Invoke-RestMethod -Method Post "$base/admin/ott-links/import-csv?path=output/kinolights_ott_links_10_with_theaters.csv"
```

## 결과 파일

- 후보: `output/kinolights_candidates_200.csv`
- 크롤링 결과: `output/kinolights_ott_links_200.csv`

크롤링 결과 CSV 주요 컬럼:

- `movie_id`, `movie_cd`, `title`
- `kinolights_url`
- `status`: `SUCCESS`, `NO_TITLE`, `NO_LINK`, `FAILED`
- `provider`
- `watch_url`
- `raw_url`, `raw_text`, `source_method`
- `is_external_direct`
- `error`, `crawled_at`

## 확인용 SQL

```sql
SELECT status, COUNT(*) AS movie_count
FROM movie_ott_crawl_status
GROUP BY status
ORDER BY status;
```

```sql
SELECT provider_name, COUNT(*) AS link_count
FROM movie_ott_link
GROUP BY provider_name
ORDER BY link_count DESC;
```

```sql
SELECT m.id, COALESCE(m.title, m.movie_name) AS title, s.status, s.error_message
FROM movie_ott_crawl_status s
JOIN movie m ON m.id = s.movie_id
WHERE s.status IN ('NO_TITLE', 'NO_LINK', 'FAILED')
ORDER BY s.updated_at DESC;
```

## 주의사항

- 대량 실시간 크롤링 금지. 결과는 DB 캐시로만 사용한다.
- URL은 사람이 샘플 클릭 검증해야 한다.
- 200개 실행은 3~7초 지연 기준으로 대략 10~25분 걸릴 수 있다.
- 키노라이츠 검색 매핑은 자동 추정이므로 `NO_TITLE` 또는 이상한 매핑은 수동 보정 대상이다.
