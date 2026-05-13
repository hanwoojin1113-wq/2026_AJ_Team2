# CineMatch

---

## 05-08 구현사항

### 실시간 인기 작품 — TMDB trending/movie/day 연동

기존 `TmdbTrendingChart`가 KOBIS 박스오피스만 사용하던 것을 TMDB trending 스냅샷 우선 → KOBIS fallback 구조로 전환했습니다.

#### 데이터 우선순위

1. **TMDB trending 스냅샷** (`tmdb_trending_chart` 테이블): TMDB 토큰 설정 시 서버 시작 때 24시간 경과 여부 확인 후 자동 갱신
2. **KOBIS 일별 박스오피스** (TMDB 스냅샷 없을 때 fallback): `KOBIS_API_KEY` 환경변수 필요

#### TMDB 갱신 정책 (`TmdbTrendingService.java`)

- `@EventListener(ApplicationReadyEvent.class)` 로 서버 시작 시 자동 실행
- 이미 DB에 있는 영화는 재사용, 없는 영화는 import → normalize → 태그 생성 후 편입
- 수동 강제 갱신: `POST /admin/pipeline/tmdb-trending/refresh`

**변경 파일:**

| 파일                                      | 설명                                             |
| ----------------------------------------- | ------------------------------------------------ |
| `chart/algorithms/TmdbTrendingChart.java` | KOBIS 단독 → TMDB 우선 + KOBIS fallback으로 전환 |
| `tmdb/TmdbTrendingService.java`           | TMDB trending/movie/day 갱신 서비스              |

---

### 취향 유사도 도넛 차트 (타 유저 프로필)

팔로잉한 유저의 프로필 페이지(`/users/{loginId}`)에 취향 유사도를 도넛 게이지로 표시합니다.

- 유사도 계산: `user_preference_profile` 테이블의 feature 벡터를 이용한 **코사인 유사도** (0–100 정수)
- SVG `stroke-dasharray` 단일 아크로 게이지 구현, 파란색(`#0048FF`) 채움
- 데이터 없으면 위젯 숨김

**변경 파일:**

| 파일                          | 설명                                                                          |
| ----------------------------- | ----------------------------------------------------------------------------- |
| `MovieController.java`        | `computeCosineSimilarity()` 메서드 추가, `similarityPct` model attribute 주입 |
| `templates/user-profile.html` | 도넛 게이지 위젯 HTML/CSS/JS 추가                                             |

---

### 메인 페이지 카드 호버 줌 + 액션 버튼

랭킹 페이지에만 있던 카드 호버 줌 효과와 액션 버튼을 메인 피드 캐러셀에도 적용했습니다.

- 호버 시 `scale(1.32)` 확대
- 확대 카드 가로 잘림 문제 해결: `scrollLeft` + `overflow-x: auto` → `transform: translateX` + `overflow-x: clip`
- 액션 버튼 4종: 좋아요 / 싫어요 / 나중에 보기 / 컬렉션 추가

---

### 액션 버튼 아이콘 통일

영화 상세 페이지(`movie-detail.html`) 기준으로 랭킹·메인 페이지의 카드 액션 버튼 SVG를 통일했습니다.

| 버튼        | 아이콘                  |
| ----------- | ----------------------- |
| 좋아요      | 하트 path               |
| 싫어요      | 하트 + 사선 path        |
| 나중에 보기 | 시계 circle + 바늘 path |
| 컬렉션      | 북마크 path             |

---

### 랭킹 차트 UI: 캐러셀 → 5×2 그리드

랭킹 홈(`/ranking`) 각 차트 섹션의 영화 배열 방식을 변경했습니다.

| 구분        | 변경 전             | 변경 후          |
| ----------- | ------------------- | ---------------- |
| 레이아웃    | 단일 행 수평 캐러셀 | 5열 × 2행 그리드 |
| 포스터 크기 | 소형                | 대형             |
| 스크롤 버튼 | 좌/우 버튼          | 없음 (전체 노출) |

반응형: 960px 이하 → 3열, 768px 이하 → 2열

---

### 랭킹 순위 누락 버그 수정

SQL `LIMIT 10` 적용 후 Java에서 포스터 URL 없는 영화를 필터링해 중간 순위가 빠지는 버그를 수정했습니다.  
`AbstractJdbcChartAlgorithm.runQuery()`의 포스터 필터 제거. 포스터 없는 경우 템플릿의 `card-poster-fallback`이 영화명으로 대체 표시합니다.

영향 범위: `AbstractJdbcChartAlgorithm`을 상속하는 **모든 15개 차트 알고리즘**

---

### 05-08 변경 파일 목록

| 파일                                      | 유형 | 설명                                             |
| ----------------------------------------- | ---- | ------------------------------------------------ |
| `MovieController.java`                    | 수정 | 코사인 유사도 계산 메서드 추가                   |
| `chart/AbstractJdbcChartAlgorithm.java`   | 수정 | 포스터 URL 필터 제거 (순위 누락 버그 수정)       |
| `chart/algorithms/TmdbTrendingChart.java` | 수정 | TMDB 트렌딩 연동 + KOBIS fallback                |
| `templates/user-profile.html`             | 수정 | 취향 유사도 도넛 차트 위젯 추가                  |
| `templates/ranking.html`                  | 수정 | 카드 아이콘 통일, 캐러셀 → 5×2 그리드            |
| `templates/ranking-detail.html`           | 수정 | 카드 아이콘 통일                                 |
| `templates/index.html`                    | 수정 | 카드 호버 줌 + 액션 버튼, translateX 캐러셀 전환 |

---

## 05-05 구현사항

### 마이페이지 UI 전면 개편

#### 취향 프로필 도넛 차트 (히어로 우측)

- 히어로 영역 우측에 사용자의 장르 취향을 시각화하는 도넛 차트 위젯을 추가했습니다.
- 서버에서 `user_movie_like`, `user_movie_watch` 기반으로 장르별 카운트 합계를 집계합니다.
- Thymeleaf `th:inline="javascript"`가 Java Record를 직렬화하지 못하는 문제를 **HTML `data-*` 속성**으로 우회해 해결했습니다.
  - `<span th:each="item : ${tasteChart}" th:attr="data-genre=${item.genre()},data-count=${item.count()},data-pct=${item.pct()}">` 방식
- SVG `stroke-dasharray` + `rotate` transform으로 비율에 따른 도넛 슬라이스를 JS로 그립니다.
- 차트 슬라이스에 마우스를 올리면 장르명 + 횟수 + 비율 툴팁이 표시됩니다.
- 취향 데이터가 없으면 "아직 감상 기록이 없습니다" 안내 문구를 표시합니다.

#### 팔로워 / 팔로잉 버튼 위치 변경

- 기존 히어로 하단 외부에 있던 팔로워/팔로잉 버튼을 **히어로 내부**로 이동했습니다.
- 인원수·좋아요 등 요약 카드 바로 위, 프로필 정보 아래에 배치했습니다.

#### 요약 카드 아이콘 코딩

- 7개 요약 카드(인원수, 좋아요, 컬렉션, 나중에볼게, 별로요, 보더라, 봤어요)에 영화 상세 페이지 버튼 활성화 상태와 동일한 그라데이션을 적용했습니다.
- 각 카드마다 `.life`, `.like`, `.coll`, `.later`, `.bad`, `.watch`, `.done` CSS 클래스로 구분합니다.

**변경 파일:**

- `MovieController.java` → `fetchUserGenreChart()`, `fetchMoviesByTag()` 메서드 추가, `tasteChart` 모델 속성 주입
- `templates/my-page.html` → 도넛 차트 위젯, 팔로워/팔로잉 위치 변경, 카드 아이콘 코딩

---

### 회원가입 Cold-start 해소 → 온보딩 페이지

회원가입 직후 취향이 전혀 없는 신규 사용자의 초기 추천 품질 문제(cold-start)를 해결하기 위해 2단계 가입 흐름을 도입했습니다.

#### 흐름

1. `POST /signup` → 기본 가입 완료 후 세션 생성 → `/onboarding` 리다이렉트
2. `/onboarding` 페이지에서 5가지 태그(funny / tense / dark / emotional / romantic)별로 영화 5편씩, 총 25개 포스터를 보여줌
3. 사용자가 마음에 드는 포스터를 클릭(다중 선택) → "선택 완료" 제출
4. `POST /onboarding` → 선택한 영화들을 `user_movie_like (liked=TRUE)`로 DB에 저장 → `/` 홈으로 이동
5. "건너뛰기" 선택 시 선택 없이 바로 홈으로 이동

#### 구현 상세

- `MovieController.java`에 `ONBOARDING_TAGS`, `GET /onboarding`, `POST /onboarding` 추가
- `OnboardingTagGroup` Record로 태그 이름 + 영화 목록을 묶어 템플릿에 전달
- 카드 클릭 시 `.selected` CSS 클래스 토글 + hidden `<input name="movieCodes">` 동적 추가/제거
- 하단 스티키 푸터: 현재 선택 수 표시 + "건너뛰기" + "선택 완료 →" 버튼

**변경 파일:**

- `MovieController.java` → 온보딩 라우트 및 DB 저장 로직 추가
- `templates/onboarding.html` → 신규 생성

---

### 메인 홈 → 인스타그램 스타일 좌측 고정 사이드바

- 홈 페이지(`/`) 좌측에 `position: fixed` 사이드바를 추가했습니다.
- 아이콘 3개: 홈(집 모양), 알림(종 모양), 프로필(사람 모양)
- 알림 버튼 클릭 시 `/api/notifications` API를 호출해 기존 상단 네비게이션과 동일한 알림 패널을 표시합니다.
- 미확인 알림이 있으면 사이드바 벨 아이콘에 파란 배지를 표시하고, 상단 네비게이션 배지와 동기화됩니다.
- 사이드바 너비 72px, `.page` 컨테이너에 `padding-left: 90px` 적용해 콘텐츠 영역이 가려지지 않도록 처리했습니다.

**변경 파일:**

- `templates/index.html` → `<aside class="sidebar">` 추가 및 CSS/JS 연동

---

### 랭킹 페이지 → 넷플릭스 스타일 캐러셀 + 인터레이스 카드 확대

#### 가로 슬라이딩 캐러셀

- 각 차트 섹션을 가로 스크롤 캐러셀로 변경했습니다.
- 좌/우 화살 버튼 클릭 시 `translateX`로 트랙을 이동합니다 (native scroll 미사용).
- `DOMMatrix`로 현재 `translateX` 값을 읽어 경계 이탈을 방지합니다.
- 전체보기 링크는 `ranking-detail.html`로 연결됩니다.

#### CSS `overflow-x: clip` → 카드 확대 클리핑 문제 해결

- `overflow-x: auto/hidden`을 사용하면 CSS 명세상 `overflow-y`도 강제로 `auto`가 되어 `transform: scale()` 시각적 출력이 잘립니다.
- **`overflow-x: clip`** (CSS Overflow Level 3)은 `overflow-y: visible`을 유지할 수 있어, 가로 스크롤 영역 밖으로 카드가 나가지 않으면서 카드가 위/아래로 확대되는 것을 허용합니다.
- `.carousel-clip { overflow-x: clip; overflow-y: visible; }` 적용.
- `.movie-card:hover { transform: scale(1.38); z-index: 20; transition: transform 0.22s ease; }`

#### 카드 호버 액션 버튼

- 포스터 호버 시 `.card-hover` 오버레이가 나타나며 4가지 액션 버튼을 제공합니다 (좋아요, 별로요, 나중에볼게, 컬렉션에 추가).
- 각 버튼 클릭 시 `POST /api/movies/{code}/{action}` API를 호출합니다.

---

### 랭킹 상세 페이지 → 렌더링 버그 수정

- `ranking-detail.html`에서 "전체보기"를 눌렀을 때 영화 목록이 아무것도 표시되지 않는 버그를 수정했습니다.
- 원인: `th:each` 루프 내 `<a>` 태그에 `th:href` 속성이 누락되고 닫는 `>` 기호도 빠져있어 Thymeleaf가 영화 카드를 하나도 렌더링하지 못했습니다.
- 수정: `th:href="@{|/movies/${row.movieCode()}|}"` 추가 및 태그 구문 교정.
- 카드 호버 시 동일한 4가지 액션 버튼을 표시하도록 개선했습니다.

---

### 05-05 변경 파일 목록

| 파일                            | 유형 | 설명                                                       |
| ------------------------------- | ---- | ---------------------------------------------------------- |
| `MovieController.java`          | 수정 | 온보딩 라우트, 장르 차트 집계, TasteChartEntry Record 추가 |
| `templates/my-page.html`        | 수정 | 도넛 차트 위젯, 팔로워/팔로잉 위치 변경, 카드 아이콘 코딩  |
| `templates/onboarding.html`     | 신규 | Cold-start 해소용 태그 기반 영화 선택 UI                   |
| `templates/index.html`          | 수정 | 인스타그램 스타일 좌측 고정 사이드바                       |
| `templates/ranking.html`        | 수정 | 넷플릭스 스타일 캐러셀 + 인터레이스 카드 확대              |
| `templates/ranking-detail.html` | 수정 | th:href 렌더링 버그 수정 + 호버 액션 버튼                  |

---

## 05-04 구현사항

### TMDB Trending 일 단위 갱신

- TMDB `trending/movie/day` Top 10을 하루 단위로 갱신하는 기능을 추가했습니다.
- 서버 시작 시 마지막 갱신 시점을 확인하고, 하루 이상 지났을 때만 새 데이터를 다시 가져오도록 구성했습니다.
- 이미 DB에 있는 영화는 재사용하고, 없는 영화는 서비스 내부 데이터로 편입할 수 있도록 확장했습니다.
- 관리자용 수동 갱신 endpoint도 함께 추가했습니다.

### 영화 상세 페이지 리뷰 기능

- 영화 상세 페이지에서 텍스트 리뷰를 작성할 수 있는 기능을 추가했습니다.
- 리뷰는 `봤어요(WATCHED)` 상태인 사용자만 등록할 수 있습니다.
- 영화별 리뷰 목록을 조회할 수 있으며, 리뷰 좋아요 기능도 함께 지원합니다.
- 리뷰 좋아요 순위에 따라 인기 리뷰를 확인할 수 있습니다.

### 영화 태그 기반 게시물 기능

- 사용자가 특정 영화를 태그해서 게시물을 작성할 수 있는 기능을 추가했습니다.
- 게시물 작성 시 영화 검색 후 태그할 수 있으며, 이미지와 설명을 함께 등록할 수 있습니다.
- 게시물은 영화와 연결되어 영화 상세 페이지에서 함께 노출됩니다.
- 게시물 작성 시 이미지를 최대 5장까지 업로드할 수 있도록 확장했습니다.
- 첫 번째 이미지가 대표 이미지로 사용되며, 게시물 상세에서 이미지 캐러셀 형태로 확인할 수 있습니다.
- 영화별 게시물 피드에서 여러 사용자의 게시물을 인스크롤 형태로 표시할 수 있습니다.

### 게시물 상세 페이지

- `/posts/{postId}` 게시물 상세 페이지를 추가했습니다.
- 작성자 정보, 게시물 이미지, 설명, 태그된 영화 카드 등을 확인할 수 있습니다.
- 본인 게시물이 아닌 경우 게시물 상세에서 바로 팔로우/언팔로우가 가능합니다.
- 게시물 좋아요 기능도 함께 지원합니다.

### 상단바 게시물 작성 버튼

- 홈 페이지 상단 네비게이션에 `+` 버튼을 추가했습니다.
- 버튼 클릭 시 게시물 작성 페이지로 이동할 수 있습니다.

### 알림 시스템 확장

기존 알림 기능을 확장해 다음 알림을 추가했습니다.

- 내가 팔로우한 사용자의 새 게시물 알림
- 내 게시물에 대한 좋아요 알림
- 내 리뷰에 대한 좋아요 알림

알림 클릭 시 관련 게시물, 영화 상세 페이지, 영화별 게시물 피드로 바로 이동할 수 있습니다.

### 소셜 인터랙션 강화

- 게시물 좋아요, 리뷰 좋아요, 팔로우 기능이 서로 연결되도록 확장했습니다.
- 영화 상세 페이지, 게시물 상세 페이지, 영화별 게시물 피드에서 사용자 간 상호작용이 가능하도록 구성했습니다.

---

## 04-30 변경사항 (2차 — 차트 알고리즘 확장 & 랭킹 UI 개편)

### 새 차트 알고리즘 4개 추가

기존 11개 차트에서 **15개**로 확장했습니다.

#### 1. 평점 명작 (`high-rated`) — `HighRatedChart.java`

- **카테고리**: `RATING` (신규 카테고리)
- **기준**: TMDB `vote_average` 내림차순, `vote_count >= 200` 필터
- **특징**: 박스오피스 흥행과 무관하게 관객 투표로 검증된 영화만 선별. 평점 8.5 이상이면 `★ 명작`, 8.0 이상이면 `★ 수작` 뱃지 표시

#### 2. 세계 흥행 순위 (`global-hit`) — `GlobalHitChart.java`

- **카테고리**: `BOXOFFICE`
- **기준**: TMDB `revenue`(달러 기준) 내림차순, revenue > 0 필터
- **특징**: 국내 박스오피스 데이터와 다른 시각으로 전 세계가 선택한 흥행작 제공. 10억 달러 이상 영화에 `10억$+` 뱃지 표시

#### 3. 주연 배우 대표작 (`actor`) — `ActorChart.java`

- **카테고리**: `PEOPLE`
- **기준**: `movie_actor.display_order <= 3` 기준 주연 배우가 2편 이상 흥행작에 출연한 경우만 포함. 배우별 누적 관객 합계 내림차순
- **특징**: 기존 `DirectorChart`와 동일한 집계 패턴을 배우에 적용. 배우명을 뱃지로 표시

#### 4. 유저 추천 영화 (`most-liked`) — `UserLikedChart.java`

- **카테고리**: `COMMUNITY` (신규 카테고리)
- **기준**: 서비스 내 `user_movie_like.liked = TRUE` 집계 내림차순
- **특징**: KOBIS / TMDB 외부 데이터가 아닌, 이 서비스 사용자들이 직접 좋아요를 누른 영화를 보여주는 커뮤니티 기반 차트

---

### ChartCategory 확장

- **파일**: `src/main/java/com/cinematch/chart/ChartCategory.java`
- `RATING("평점")` 추가 → 평점 명작 차트를 위한 신규 카테고리
- `COMMUNITY("커뮤니티")` 추가 → 유저 활동 기반 차트를 위한 신규 카테고리
- 카테고리 탭 자동으로 포함됨 (`ChartController`가 `ChartCategory.values()`를 동적으로 읽음)

---

### 랭킹 UI 전면 개편 — 헤브라이즌 스타일 포스터 그리드

#### `ranking.html` 변경사항

| 항목          | 변경 전                                | 변경 후                                                     |
| ------------- | -------------------------------------- | ----------------------------------------------------------- |
| 기본 레이아웃 | 순위번호 + 작은 포스터 + 텍스트 리스트 | 포스터 그리드 (5열, 2:3 비율)                               |
| 순위 번호     | 왼쪽 고정 숫자                         | 포스터 하단 오버레이 (금/은/동 색상)                        |
| 뱃지          | 포스터 좌상단 항상 노출                | 포스터 좌상단 오버레이                                      |
| 지표 표시     | 오른쪽 이어 항상 노출                  | 호버 시 포스터 위의 오버레이로 등장                         |
| 모바일        | 리스트 축소                            | 3열 그리드 (480px 이하 2열)                                 |
| 뷰 전환       | 없음                                   | 그리드 ↔ 리스트 토글 버튼 추가 (선택값 `localStorage` 유지) |

#### `ranking-detail.html` 변경사항

| 항목           | 변경 전                               | 변경 후                               |
| -------------- | ------------------------------------- | ------------------------------------- |
| 레이아웃       | 카드형 리스트 (순위+포스터+정보+지표) | 포스터 그리드 (6열)                   |
| 뷰 전환        | 없음                                  | 그리드 ↔ 리스트 토글 (우상단)         |
| 결과 수 표시   | 없음                                  | "총 N개 영화" 카운트 표시             |
| 빈 결과 메시지 | 단순 문구                             | "TMDB 동기화 후 채워집니다" 안내 추가 |

---

### 04-30 (2차) 변경 파일 목록

| 파일                                   | 유형 | 설명                                      |
| -------------------------------------- | ---- | ----------------------------------------- |
| `chart/ChartCategory.java`             | 수정 | RATING, COMMUNITY 카테고리 추가           |
| `chart/algorithms/HighRatedChart.java` | 신규 | 평점 명작 차트                            |
| `chart/algorithms/GlobalHitChart.java` | 신규 | 세계 흥행 순위 차트                       |
| `chart/algorithms/ActorChart.java`     | 신규 | 주연 배우 대표작 차트                     |
| `chart/algorithms/UserLikedChart.java` | 신규 | 유저 추천 영화 차트                       |
| `templates/ranking.html`               | 수정 | 전면 재설계 → 헤브라이즌 스타일 그리드    |
| `templates/ranking-detail.html`        | 수정 | 전면 재설계 → 포스터 그리드 + 리스트 토글 |

---

## 04-30 변경사항 (1차)

- TMDB Trending API를 연동해 `실시간 인기 작품` 카테고리를 추가
  - TMDB `trending/movie/week` 기준 상위 10개 영화를 가져옴.
  - 가져온 영화는 서비스 DB에 중복 없이 편입하고, chart 최상단 독립 카테고리로 노출
- 공연/콘서트/스포츠 성격의 콘텐츠를 추천 대상과 게임 노출에서 제외
- 메인 화면에 `{사용자명}님의 인생영화` 카테고리를 추가
  - `user_preference_profile` 기반 코사인 유사도로 비슷한 사용자를 찾고,
  - 유사 사용자의 `인생영화`만 모아 별도 필터링 추천으로 노출
- 기존 콘텐츠 기반 개인화 추천 알고리즘의 품질을 보완
  - 낮은 별점 (1~2점)을 부정 신호로 반영
  - 배우/감독 선호 노이즈 축소
  - 장르/태그 편향 완화
  - 애니메이션 장르 과편향 추가 개선
  - 개인화 추천 블록의 중복/반복을 줄이도록 diversity를 강화
- 다른 사용자의 공개 프로필 페이지 추가
  - 프로필 사진, 사용자 ID/이름, 팔로워/팔로잉 수, 팔로우/언팔로우 버튼
  - 인생영화, 보더라, 봤어요/별점, 좋아요 목록에 이동할 수 있습니다.
- 친구/팔로워/팔로잉/추천 친구 목록에서 프로필 사진과 사용자명 클릭 시 해당 사용자의 공개 프로필로 이동하도록 연결했습니다.

---

# CineMatch — 프로젝트 전체 설명

영화 메타데이터와 사용자 행동 기록을 기반으로 개인화 추천을 제공하는 Spring Boot 기반 영화 추천 웹 프로젝트입니다.

이 프로젝트는 단순 영화 조회 사이트가 아니라, 아래 흐름이 하나로 연결된 서비스로 구성되어 있습니다.

- KOBIS / TMDB 영화 데이터 수집
- 영화 메타데이터 정규화
- 추천용 태그 자동 생성
- 사용자 행동 기반 취향 프로필 생성
- 전체 개인화 랭킹 생성
- 랭킹 기반 추천 블록 제공

현재 코드 기준 패키지 루트는 `com.cinematch` 입니다.

---

## 프로젝트 개요

### 목표

- 사용자의 취향에 맞는 영화를 추천한다.
- 영화가 실제로 어느 OTT에서 제공되는지 함께 보여준다.
- 좋아요, 봤어요, 나중에 볼거야, 인생영화 같은 명시적 행동을 추천에 반영한다.
- 메인 홈은 추천 중심, 마이페이지는 행동 기록 중심으로 구성한다.
- 추천 결과를 장르/배우/감독/태그/OTT 블록 형태로 재구성해 제공한다.

### 핵심 차별점

- 장르만 쓰지 않고 추천용 태그 레이어를 별도로 만든다.
- 사용자 행동에 가중치를 두어 `취향 프로필`로 변환한다.
- 전체 개인화 랭킹 1개를 먼저 만든 다음, 이를 다시 분류해 추천 블록을 만든다.
- 추천 결과에 `reason_summary`를 저장해 설명 가능성을 확보한다.

---

## 현재 구현 기능

### 웹 기능

- 로그인 / 회원가입 / 온보딩
- 메인 홈 추천 row UI
- 영화 상세 페이지
- 마이페이지
- 팔로우 / 팔로잉 기반 소셜 기능
- 게시물 작성 / 리뷰 / 좋아요
- 관리자 배치 실행 API
- 추천 검증용 더미 사용자 / 더미 행동 데이터

### 영화 상세 페이지 액션

| 액션            | 설명                                        |
| --------------- | ------------------------------------------- |
| 별로요          | 부정 신호                                   |
| 좋아요          | 긍정 신호                                   |
| 나중에 볼거야   | 관심 신호                                   |
| 보더라 / 봤어요 | `OFF → WATCHING → WATCHED → OFF` 3단계 토글 |
| 별점            | 봤어요 상태에서만 1~5점 가능                |
| 컬렉션 추가     | 컬렉션에 영화 추가                          |

- 좋아요 / 별로요는 상호배타
- 버튼 클릭은 AJAX 기반이라 뒤로가기 히스토리가 남지 않도록 처리

---

## 기술 스택

- Java 21, Spring Boot 3, Maven
- JdbcTemplate, H2 Database (MySQL 호환 모드)
- Thymeleaf, 순수 HTML/CSS/JS, Pretendard 폰트
- KOBIS Open API, TMDB API

---

## 실행 방법

```powershell
# 환경 변수 설정 (없으면 외부 API 기능 비활성화)
$env:KOBIS_API_KEY="..."
$env:TMDB_TOKEN="..."

.\mvnw.cmd spring-boot:run
```

기본 포트: `http://localhost:8080`

H2 콘솔: `http://localhost:8080/h2-console`  
DB 경로: `./data/kobisdb.mv.db`

---

## 더미 계정

추천 검증용 더미 계정이 준비되어 있습니다.

- 계정: `testuser01` ~ `testuser16`
- 비밀번호: `Test1234!`

---

## 프로젝트 구조

```
src/main/java/com/cinematch
├── admin            # 배치/자동화/검증용 관리자 기능
├── chart            # 랭킹 차트 기능
│   └── algorithms/  # 개별 차트 구현체 (15개)
├── kobis            # KOBIS raw import / normalize
├── recommendation   # 취향 프로필 / 랭킹 / 블록 / refresh
├── tag              # 추천용 태그 생성
├── tmdb             # TMDB raw import / normalize / trending
├── MovieController  # 로그인 이후 주요 화면과 액션 처리
└── LoginApplication # Spring Boot 시작점
```

---

## 데이터 처리 파이프라인

1. KOBIS raw 수집 → 2. KOBIS 정규화 → 3. TMDB raw 수집 → 4. TMDB 정규화 → 5. 추천용 태그 생성 → 6. 사용자 행동 데이터 결합 → 7. 추천 프로필 / 랭킹 / 블록 생성

- **KOBIS**: 국내 영화 기본 골격 생성 (movie, movie_genre, movie_director, movie_actor 등)
- **TMDB**: KOBIS가 만든 영화 골격에 상세 메타데이터 보강 (포스터, 오버뷰, 평점, 키워드, OTT 제공처)

---

## 태깅 시스템

장르만으로는 추천 품질이 쉽게 떨어지기 때문에, 별도의 추천용 태그 레이어를 만듭니다.

**핵심 파일**: `MovieTagService.java`, `MovieTagGenerator.java`, `DefaultRecommendationTagRules.java`

**주요 태그 예시**: `tense`, `mystery`, `dark`, `healing`, `romantic`, `with_family`, `with_partner`, `investigation`, `coming_of_age`, `true_story`, `survival`, `revenge`

- 성인 영화, 장르 없는 영화, 공연/콘서트 계열 제외
- 태그 점수 threshold를 넘어야만 확정 태그로 채택
- 태그 저장: `tag` (마스터), `movie_tag` (영화-태그 연결)

---

## 추천 알고리즘 개요

현재 추천은 **규칙 기반 태그 중심 하이브리드 콘텐츠 추천**입니다.

**핵심 파일**: `UserPreferenceProfileService.java`, `RecommendationRankingService.java`, `RecommendationBlockService.java`

### 가중치 우선순위 (현재 정책값)

| 요소               | 가중치 |
| ------------------ | ------ |
| TAG                | 0.34   |
| GENRE              | 0.18   |
| PEOPLE (감독+배우) | 0.16   |
| KEYWORD            | 0.08   |
| POPULARITY         | 0.10   |
| FRESHNESS          | 0.04   |
| PROVIDER           | 0.02   |

### 사용자 선호 가중치

| 행동            | 가중치     |
| --------------- | ---------- |
| 인생영화        | 5.0        |
| 좋아요          | 3.0        |
| 봤어요 (기본)   | 2.2 × 0.70 |
| 봤어요 + 별점 5 | 2.2 × 1.20 |
| 나중에볼게      | 1.5        |

### 추천 블록 생성

전체 랭킹에서 slice를 가져와 TAG/GENRE/DIRECTOR/ACTOR/PROVIDER 기준으로 재묶어 블록 단위로 노출합니다. 블록 최소 크기를 만족하지 못하면 노출하지 않습니다.

---

## 관리자 배치 엔드포인트

| 엔드포인트                                   | 설명                                         |
| -------------------------------------------- | -------------------------------------------- |
| `POST /admin/pipeline/tmdb-trending/refresh` | TMDB 트렌딩 수동 갱신                        |
| `POST /admin/pipeline/...`                   | KOBIS/TMDB import, normalize, 태그 재생성 등 |
