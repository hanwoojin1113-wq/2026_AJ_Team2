# Kinolights OTT Link PoC

## 목적

CineMatch 영화 상세 페이지에서 OTT 버튼을 눌렀을 때 해당 OTT의 작품 페이지로 이동할 수 있는지 확인하기 위한 소규모 PoC입니다.

이 스크립트는 Spring Boot 서비스 코드, DB schema, 템플릿을 수정하지 않습니다. 수동으로 준비한 키노라이츠 작품 URL 목록을 Playwright로 렌더링하고, 공개 페이지의 `보러가기` 주변에서 OTT 제공사 URL 후보를 CSV로 캐싱합니다.

## 운영 원칙

- 공개 작품 페이지 URL만 대상으로 합니다.
- 로그인, 캡차, 차단 우회, 비공개 API 호출은 하지 않습니다.
- 서비스 요청 시점에 크롤링하지 않습니다.
- 100개 내외 샘플을 저빈도로 수집한 뒤 결과 CSV/DB 캐시만 서비스에서 사용합니다.
- 결과 URL은 사람이 샘플 클릭 검증해야 합니다.

## 설치

PowerShell 기준:

```powershell
python -m pip install playwright
python -m playwright install chromium
```

## 입력 CSV

기본 입력 파일:

```text
scripts/kinolights_titles.csv
```

형식:

```csv
title,kinolights_url
기생충,https://m.kinolights.com/title/66957
파묘,https://m.kinolights.com/title/125723
```

`scripts/kinolights_titles.csv`가 없으면 샘플 파일 `scripts/kinolights_titles.sample.csv`를 사용합니다. 실제 검증 전에는 URL이 현재 키노라이츠 작품과 맞는지 직접 확인하세요.

## 실행

기본 실행:

```powershell
python scripts/kinolights_ott_crawler.py
```

100개 이하로 제한하고 결과 파일 지정:

```powershell
python scripts/kinolights_ott_crawler.py --input scripts/kinolights_titles.csv --output output/kinolights_ott_links.csv --max-items 100
```

브라우저를 눈으로 보면서 디버깅:

```powershell
python scripts/kinolights_ott_crawler.py --headful --max-items 5
```

요청 간 딜레이 조정:

```powershell
python scripts/kinolights_ott_crawler.py --delay-min 5 --delay-max 10
```

## 출력 CSV

기본 출력:

```text
output/kinolights_ott_links.csv
```

컬럼:

- `title`: 입력 영화 제목
- `kinolights_url`: 입력 키노라이츠 URL
- `provider`: 감지된 OTT 제공사
- `watch_url`: 외부 direct URL로 판단된 URL
- `raw_url`: 원본 href 또는 클릭 후 포착 URL
- `raw_text`: 버튼/링크 텍스트
- `source_method`: `anchor`, `click_popup`, `click_navigation`
- `is_external_direct`: 키노라이츠 내부 URL이 아닌 외부 URL 여부
- `error`: 페이지 처리 실패 메시지
- `crawled_at`: CSV 생성 시각

## 결과 해석

- `watch_url`이 비어 있지 않고 `is_external_direct=true`인 행이 실제 서비스 버튼 후보입니다.
- `provider`는 잡혔지만 `watch_url`이 비어 있으면 내부 redirect 또는 버튼 구조만 확인된 상태입니다.
- `raw_url`이 키노라이츠 내부 URL이면 direct link로 쓰기 전에 클릭/리다이렉트 검증이 필요합니다.
- TVING, Wavve, Watcha, Coupang Play가 핵심 판단 대상입니다.
- Netflix, Disney+, Apple TV, Google Play, Naver SeriesOn은 보조 판단 대상입니다.
- URL이 있더라도 실제 해당 OTT의 해당 영화 상세 페이지로 이동하는지는 사람이 샘플 클릭 검증해야 합니다.

## 서비스 반영 방향

PoC 결과가 쓸 만하면 서비스에는 다음 구조가 안전합니다.

```text
movie_ott_link
- id
- movie_id
- provider_name
- watch_url
- source
- crawled_at
- verified_at
```

운영 서비스는 크롤러를 실시간으로 호출하지 않고, 검증된 `movie_ott_link` 캐시만 조회해서 영화 상세 페이지의 OTT 버튼으로 표시하는 방식이 적절합니다.

## 주의사항

이 스크립트는 포트폴리오/기능 검증 목적의 소규모 도구입니다. 대량 수집, 재판매, 차단 우회, 로그인 세션 사용, 비공개 API 호출 용도로 확장하지 마세요.
