# OTT Direct Link PoC

## 1. PoC 목적

이 PoC는 한국 OTT direct URL 품질을 비교하기 위한 독립 스크립트입니다.

비교 대상 API:
- Streaming Availability API
- Watchmode API

비교 기준:
- 단순 provider 이름 유무가 아니라, 영화 상세에서 OTT 버튼 클릭 시 해당 OTT의 해당 영화 페이지로 이동 가능한 direct URL / deep link가 실제로 제공되는지입니다.

핵심 한국 OTT:
- TVING
- Wavve
- Watcha
- Coupang Play

보조 OTT:
- Netflix
- Disney+
- Apple TV
- Google Play
- Naver SeriesOn

## 2. 필요한 API 키

필수:
- `TMDB_TOKEN` 또는 `tmdb.token`

선택:
- `STREAMING_AVAILABILITY_API_KEY` 또는 `streaming.availability.api-key`
- `WATCHMODE_API_KEY` 또는 `watchmode.api-key`
- `RAPIDAPI_HOST` 또는 `rapidapi.host`

스크립트 우선순위:
1. 환경변수
2. `src/main/resources/application-local.properties`

권장 방식:
- TMDB / KOBIS / Streaming Availability / Watchmode 키를 모두 `application-local.properties`에서 같이 관리
- 필요할 때만 환경변수로 덮어쓰기

## 3. application-local.properties 예시

파일:

`src/main/resources/application-local.properties`

예시:

```properties
kobis.api-key=...
tmdb.token=...
streaming.availability.api-key=...
# watchmode.api-key=
# rapidapi.host=
```

`application-local.properties`는 이미 `.gitignore`에 포함되어 있으므로 로컬 전용 키 관리용으로 쓰기 좋습니다.

## 4. PowerShell 환경변수 설정 방법

### TMDB + Streaming Availability만 테스트

```powershell
$env:TMDB_TOKEN="YOUR_TMDB_V4_TOKEN"
python scripts/ott_link_poc.py
```

### TMDB + Streaming Availability + Watchmode 테스트

```powershell
$env:TMDB_TOKEN="YOUR_TMDB_V4_TOKEN"
$env:WATCHMODE_API_KEY="YOUR_WATCHMODE_KEY"
python scripts/ott_link_poc.py
```

### Streaming Availability를 RapidAPI 경유로 테스트

```powershell
$env:TMDB_TOKEN="YOUR_TMDB_V4_TOKEN"
$env:STREAMING_AVAILABILITY_API_KEY="YOUR_RAPIDAPI_KEY"
$env:RAPIDAPI_HOST="streaming-availability.p.rapidapi.com"
python scripts/ott_link_poc.py
```

### application-local.properties만 사용해서 실행

```powershell
python scripts/ott_link_poc.py
```

## 5. 실행 명령어

```powershell
python scripts/ott_link_poc.py
```

`requests`가 없다면 먼저 설치:

```powershell
python -m pip install requests
```

## 6. 결과 CSV 위치

실행 결과 CSV:

```text
output/ott_link_poc_results.csv
```

스크립트가 실행 시 `output/` 디렉터리를 자동 생성합니다.

## 7. 결과 해석 기준

우선순위:
1. 한국 핵심 OTT direct URL이 많이 잡히는 API가 우선 후보입니다.
2. TVING / Wavve / Watcha / Coupang Play가 가장 중요합니다.
3. Netflix / Disney+ / Apple TV는 보조 판단 기준입니다.
4. provider 이름만 있고 URL이 없으면 핵심 기능에는 부족합니다.
5. URL이 있더라도 실제 해당 OTT의 해당 영화 페이지로 이동하는지는 사람이 샘플 클릭 검증해야 합니다.
6. 최종 서비스 구조는 `API 자동 수집 + 자체 DB 캐싱 + manual override 보정`이 필요합니다.

판단 예시:
- 한국 핵심 OTT direct link가 20개 이상이면 자동 API로 사용할 가치가 높습니다.
- 10~19개면 API + manual override 구조가 필요합니다.
- 10개 미만이면 API는 보조로만 쓰고 manual override 중심이 현실적입니다.

## 8. 주의사항

- 이 스크립트는 기존 Spring Boot 서비스 코드나 DB를 사용하지 않습니다.
- 테스트 영화 30개는 TMDB Search API로 직접 매칭합니다.
- 검색 결과가 여러 개인 경우 스크립트가 가장 그럴듯한 첫 후보를 선택하지만, CSV의 `tmdb_title`, `tmdb_original_title`, `tmdb_release_date`를 사람이 함께 검토해야 합니다.
- 외부 API 응답 구조는 바뀔 수 있으므로 스크립트는 가능한 범위에서 방어적으로 파싱합니다.
- API rate limit을 고려해 각 요청 사이에 짧은 sleep이 들어 있습니다.
- Watchmode 응답 구조나 플랜 제한에 따라 일부 URL 필드는 비어 있을 수 있습니다.
- Streaming Availability는 공식 문서 기준 `X-API-Key` 방식과 RapidAPI 방식을 모두 지원하도록 작성했습니다.

## 9. 참고한 공식 문서

- [TMDB API](https://developer.themoviedb.org/docs/getting-started)
- [Streaming Availability Docs](https://docs.movieofthenight.com/)
- [Streaming Availability Shows Guide](https://docs.movieofthenight.com/guide/shows)
- [Streaming Availability Localization](https://docs.movieofthenight.com/guide/localization)
- [Watchmode API](https://api.watchmode.com/)
