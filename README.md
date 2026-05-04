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
- 리뷰 좋아요 수에 따라 인기 리뷰를 확인할 수 있습니다.

### 영화 태그 기반 소셜 게시물 기능
- 사용자가 특정 영화를 태그해서 게시물을 작성할 수 있는 기능을 추가했습니다.
- 게시물 작성 시 영화 검색 후 태그할 수 있으며, 이미지와 설명을 함께 등록할 수 있습니다.
- 게시물은 영화와 연결되어 영화 상세 페이지에서 함께 노출됩니다.
- 게시물 작성 시 이미지를 최대 5장까지 업로드할 수 있도록 확장했습니다.
- 첫 번째 이미지가 대표 이미지로 사용되며, 게시물 상세에서는 이미지 캐러셀 형태로 확인할 수 있습니다.
- 영화별 게시물 피드에서는 여러 사용자의 게시물을 연속 스크롤 형태로 탐색할 수 있습니다.

### 영화 상세 페이지 게시물 연동
- 영화 상세 페이지 하단에 해당 영화를 태그한 게시물 그리드를 추가했습니다.
- 게시물 썸네일을 클릭하면 해당 영화의 게시물 피드로 이동할 수 있습니다.
- 영화와 관련된 사용자 게시물을 함께 탐색할 수 있도록 상세 페이지를 확장했습니다.

### 게시물 상세 페이지
- `/posts/{postId}` 게시물 상세 페이지를 추가했습니다.
- 작성자 정보, 게시물 이미지, 설명, 태그된 영화 카드 등을 확인할 수 있습니다.
- 본인 게시물이 아닌 경우 게시물 상세에서 바로 팔로우/언팔로우가 가능합니다.
- 게시물 좋아요 기능도 함께 지원합니다.

### 상단바 게시물 작성 버튼
- 주요 페이지 상단 네비게이션에 `+` 버튼을 추가했습니다.
- 버튼 클릭 시 게시물 작성 페이지로 이동할 수 있습니다.
- 사용자가 영화 감상 기록을 더 쉽게 게시물로 남길 수 있도록 접근성을 높였습니다.

### 알림 시스템 확장
- 기존 알림 기능을 확장해 다음 알림을 추가했습니다.
  - 내가 팔로우한 사용자의 새 게시물 알림
  - 내 게시물에 대한 좋아요 알림
  - 내 리뷰에 대한 좋아요 알림
- 알림 클릭 시 관련 게시물, 영화 상세 페이지, 영화별 게시물 피드로 바로 이동할 수 있습니다.
- 알림 패널에서 알림 종류에 따라 다른 문구와 링크가 표시되도록 개선했습니다.

### 소셜 인터랙션 강화
- 게시물 좋아요, 리뷰 좋아요, 팔로우 기능이 서로 연결되도록 확장했습니다.
- 영화 상세 페이지, 게시물 상세 페이지, 영화별 게시물 피드에서 사용자 간 상호작용이 가능하도록 구성했습니다.



## 04-30 변경사항 (2차 — 차트 알고리즘 확장 & 랭킹 UI 개편)

### 새 차트 알고리즘 4개 추가

기존 11개 차트에서 **15개**로 확장했습니다.

#### 1. 평점 명작 (`high-rated`) — `HighRatedChart.java`

- **카테고리**: `RATING` (신규 카테고리)
- **기준**: TMDB `vote_average` 내림차순, `vote_count >= 200` 필터
- **특징**: 박스오피스 흥행과 무관하게 관객 투표로 검증된 작품만 선별. 평점 8.5 이상이면 `★ 명작`, 8.0 이상이면 `★ 수작` 뱃지 표시
- **파일**: `src/main/java/com/cinematch/chart/algorithms/HighRatedChart.java`

#### 2. 세계 흥행 순위 (`global-hit`) — `GlobalHitChart.java`

- **카테고리**: `BOXOFFICE`
- **기준**: TMDB `revenue`(달러 기준) 내림차순, revenue > 0 필터
- **특징**: 국내 박스오피스 데이터와 다른 시각으로 전 세계가 선택한 흥행작 제공. 10억 달러 이상 작품에 `10억$+` 뱃지 표시
- **파일**: `src/main/java/com/cinematch/chart/algorithms/GlobalHitChart.java`

#### 3. 주연 배우 대표작 (`actor`) — `ActorChart.java`

- **카테고리**: `PEOPLE`
- **기준**: `movie_actor.display_order <= 3` 기준 주연 배우가 2편 이상 흥행작에 출연한 경우만 포함. 배우별 누적 관객 합계 내림차순
- **특징**: 기존 `DirectorChart`와 동일한 집계 패턴을 배우에 적용. 같은 배우의 여러 작품이 나올 수 있으며 배우명을 뱃지로 표시
- **파일**: `src/main/java/com/cinematch/chart/algorithms/ActorChart.java`

#### 4. 유저 추천 영화 (`most-liked`) — `UserLikedChart.java`

- **카테고리**: `COMMUNITY` (신규 카테고리)
- **기준**: 서비스 내 `user_movie_like.liked = TRUE` 집계 내림차순
- **특징**: KOBIS / TMDB 외부 데이터가 아닌, 이 서비스 사용자들이 직접 좋아요를 누른 영화를 보여주는 커뮤니티 기반 차트. 데이터가 쌓일수록 신뢰도가 높아짐
- **파일**: `src/main/java/com/cinematch/chart/algorithms/UserLikedChart.java`

---

### ChartCategory 확장

- **파일**: `src/main/java/com/cinematch/chart/ChartCategory.java`
- `RATING("평점")` 추가 — 평점 명작 차트를 위한 신규 카테고리
- `COMMUNITY("커뮤니티")` 추가 — 유저 활동 기반 차트를 위한 신규 카테고리
- 카테고리 탭 자동으로 포함됨 (`ChartController`가 `ChartCategory.values()`를 동적으로 읽음)

---

### 랭킹 UI 전면 개편 — 키노라이츠 스타일 포스터 그리드

기존 리스트 형태의 차트 UI를 **포스터 그리드 중심**으로 전면 재설계했습니다.

#### `ranking.html` 변경사항

- **파일**: `src/main/resources/templates/ranking.html`

| 항목 | 변경 전 | 변경 후 |
|------|---------|---------|
| 기본 레이아웃 | 순위번호 + 작은 포스터 + 텍스트 리스트 | 포스터 그리드 (5열, 2:3 비율) |
| 순위 번호 | 왼쪽 고정 숫자 | 포스터 하단 오버레이 (금/은/동 색상) |
| 뱃지 | 포스터 위 소형 표시 | 포스터 좌상단 오버레이 |
| 지표 표시 | 오른쪽 열에 항상 노출 | 호버 시 포스터 위에 오버레이로 등장 |
| 모바일 | 리스트 축소 | 3열 그리드 (480px 이하 2열) |
| 뷰 전환 | 없음 | 그리드 ↔ 리스트 토글 버튼 추가 (선택 값 `localStorage` 유지) |
| 새 아이콘 지원 | 없음 | `globe`, `heart`, `user` SVG 아이콘 추가 |
| 반응형 컬럼 | 고정 | 1280px→5열 / 1024px→4열 / 768px→3열 / 480px→2열 |

호버 상호작용:
- 포스터에 마우스를 올리면 `translateY(-4px)` + 그림자가 생기며 떠오름
- 지표(예: `누적 관객 1,500만 명`) 정보가 포스터 위 반투명 오버레이로 표시

#### `ranking-detail.html` 변경사항

- **파일**: `src/main/resources/templates/ranking-detail.html`

| 항목 | 변경 전 | 변경 후 |
|------|---------|---------|
| 레이아웃 | 카드형 리스트 (순위+포스터+정보+지표) | 포스터 그리드 (6열) |
| 뷰 전환 | 없음 | 그리드 ↔ 리스트 토글 (상단 우측) |
| 결과 수 표시 | 없음 | "총 N개 작품" 카운트 표시 |
| 빈 결과 메시지 | 단순 문구 | "TMDB 동기화 후 채워집니다" 안내 추가 |
| 반응형 컬럼 | 고정 | 1280px→6열 / 1024px→5열 / 768px→3열 |

---

### 변경 파일 목록

| 파일 | 유형 | 설명 |
|------|------|------|
| `chart/ChartCategory.java` | 수정 | RATING, COMMUNITY 카테고리 추가 |
| `chart/algorithms/HighRatedChart.java` | 신규 | 평점 명작 차트 |
| `chart/algorithms/GlobalHitChart.java` | 신규 | 세계 흥행 순위 차트 |
| `chart/algorithms/ActorChart.java` | 신규 | 주연 배우 대표작 차트 |
| `chart/algorithms/UserLikedChart.java` | 신규 | 유저 추천 영화 차트 |
| `templates/ranking.html` | 수정 | 전면 재설계 — 키노라이츠 스타일 그리드 |
| `templates/ranking-detail.html` | 수정 | 전면 재설계 — 포스터 그리드 + 리스트 토글 |

---

## 04-30 변경사항 (1차)

- TMDB Trending API를 연동해 `실시간 인기 작품` 카테고리를 추가
  - TMDB `trending/movie/week` 기준 상위 10개 작품을 가져옴.
  - 가져온 영화는 서비스 DB에 중복 없이 편입하고, chart 최상단 독립 카테고리로 노출
- 공연/콘서트/실황 성격의 콘텐츠는 추천 대상과 검색 노출에서 제외
- 메인 화면에 `{유저명}님의 인생영화` 카테고리를 추가
  - `user_preference_profile` 기반 코사인 유사도로 비슷한 사용자를 찾고,
  - 유사 사용자의 `인생영화`만 모아 별도 협업 필터링 추천으로 노출
- 기존 콘텐츠 기반 개인화 추천 알고리즘의 품질을 보정
  - 낮은 별점(1~2점)을 부정 신호로 반영
  - 배우/감독 신호 노이즈 축소
  - 장르/태그 편향 완화
  - 애니메이션 장르 과편향 추가 감쇠
  - 개인화 추천 블록의 중복/반복을 줄이도록 diversity를 강화
- 대표 유사 사용자명을 클릭하면 해당 사용자의 공개 프로필 페이지로 이동할 수 있습니다.
- 다른 사용자의 공개 프로필 페이지를 추가했습니다.
  - 프로필 사진, 사용자 ID/이름, 팔로워/팔로잉 수, 팔로우/팔로잉 버튼
  - 인생영화, 보는중, 봤어요/별점, 좋아요 목록을 열람할 수 있습니다.
- 친구/팔로워/팔로잉/추천 친구 목록에서 프로필 사진과 사용자명 클릭 시 해당 사용자의 공개 프로필로 이동하도록 연결했습니다.

  <img width="1666" height="599" alt="Image" src="https://github.com/user-attachments/assets/b2024636-08f8-489b-9239-ae92111ae344" />

  <img width="877" height="582" alt="Image" src="https://github.com/user-attachments/assets/e8ecd15a-5628-4ab1-b8ed-498d2f01b60f" />



# CineMatch

영화 메타데이터와 사용자 활동 기록을 기반으로 개인화 추천을 제공하는 Spring Boot 기반 영화 추천 웹 프로젝트입니다.

이 프로젝트는 단순 영화 조회 사이트가 아니라, 아래 흐름이 하나로 연결된 서비스로 구성되어 있습니다.

- KOBIS / TMDB 영화 데이터 수집
- 영화 메타데이터 정규화
- 추천용 태그 자동 생성
- 사용자 활동 기반 취향 프로필 생성
- 전체 개인화 랭킹 생성
- 랭킹 기반 추천 블록 제공

현재 코드 기준 패키지 루트는 `com.cinematch` 입니다.

---

## 1. 프로젝트 개요

### 목표

CineMatch의 목표는 다음과 같습니다.

- 사용자의 취향에 맞는 영화를 추천한다.
- 영화가 실제로 어디 OTT에서 제공되는지 함께 보여준다.
- 좋아요, 봤어요, 나중에 볼래요, 인생영화 같은 명시적 행동을 추천에 반영한다.
- 메인 홈은 추천 중심, 마이페이지는 활동 기록 중심으로 구성한다.
- 추천 결과를 장르/배우/감독/태그/OTT 블록 형태로 재구성해 제공한다.

### 핵심 차별점

- 장르만 쓰지 않고 추천용 태그 레이어를 별도로 생성한다.
- 사용자 행동을 가중치로 누적해 `취향 프로필`로 변환한다.
- 전체 개인화 랭킹 1개를 먼저 만든 뒤, 이를 다시 분류해 추천 블록을 만든다.
- 추천 결과에 `reason_summary`를 저장해 설명 가능성을 확보했다.

---

## 2. 현재 구현 기능

### 웹 기능

- 로그인 / 회원가입
- 메인 홈 추천 row UI
- 영화 상세 페이지
- 마이페이지
- 팔로우 / 팔로잉 기반 소셜 기능
- 관리자 배치 실행 API
- 추천 검증용 더미 유저 / 더미 활동 데이터

### 영화 상세 페이지 액션

영화 상세 페이지에서는 다음 액션이 동작합니다.

- 별로에요
- 좋아요
- 나중에 볼래요
- 보는중 / 봤어요
- 별점(봤어요 상태에서만)
- 컬렉션 추가

동작 특징:

- 좋아요 / 별로에요는 상호배타
- 보는중 / 봤어요는 `OFF -> WATCHING -> WATCHED -> OFF` 3단계 토글
- 봤어요 상태에서만 별점 1~5점 가능
- 버튼 클릭은 AJAX 기반이라 뒤로가기 히스토리가 덜 오염되도록 처리

### 마이페이지

마이페이지는 추천보다 활동 기록 중심입니다.

- 인생영화
- 좋아요
- 컬렉션
- 나중에 볼래요
- 별로에요
- 보는중
- 봤어요

추가로:

- 팔로워 / 팔로잉 수 표시
- 숫자 클릭 시 목록 확인
- 프로필 이미지 영역
- People 버튼을 통한 사용자 검색 / 팔로우 UX

### 소셜 기능

친구 요청/수락 방식이 아니라 팔로우 방식입니다.

- 사용자 검색
- 팔로우 / 언팔로우
- 팔로워 / 팔로잉 목록
- 프로필 이미지 fallback 표시

### 차트 기능

추천과 별도로 차트 기능도 존재합니다.

- 박스오피스 / 장르 / 감독 등 주제별 차트
- `ChartController`, `ChartRegistry`, `chart/algorithms` 패키지에서 관리

---

## 3. 기술 스택

- Java 25
- Spring Boot
- JdbcTemplate
- H2 Database
- Thymeleaf
- Maven
- KOBIS Open API
- TMDB API

---

## 4. 실행 방법

### 기본 실행

프로젝트 루트에서 실행합니다.

```powershell
.\mvnw.cmd spring-boot:run
```

### 데이터베이스

현재 프로젝트는 H2 파일 DB를 사용합니다.

`src/main/resources/application.properties`

```properties
spring.datasource.url=jdbc:h2:file:./data/kobisdb;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
```

즉 프로젝트 루트의 `data/kobisdb.mv.db`를 사용합니다.

### 외부 API 환경 변수

영화 수집 / 보강 배치를 직접 돌릴 때는 환경변수가 필요합니다.

```powershell
$env:KOBIS_API_KEY="..."
$env:TMDB_TOKEN="..."
```

### 더미 계정

추천 검증용 더미 계정이 준비되어 있습니다.

- `testuser01` ~ `testuser16`
- 비밀번호: `Test1234!`

이 더미 유저들은 스릴러형, 로맨스형, 가족/애니형, 액션형, 감독 중심형, 배우 중심형, OTT 제약형 등 서로 다른 페르소나로 설계되어 있습니다.

---

## 5. 프로젝트 구조

핵심 패키지는 다음과 같습니다.

```text
src/main/java/com/cinematch
├─ admin            # 배치/시드/검증용 관리자 기능
├─ chart            # 테마형 차트 기능
├─ kobis            # KOBIS raw import / normalize
├─ recommendation   # 취향 프로필 / 랭킹 / 블록 / refresh
├─ tag              # 추천용 태그 생성
├─ tmdb             # TMDB raw import / normalize
├─ MovieController  # 로그인 이후 주요 화면과 액션 처리
└─ LoginApplication # Spring Boot 시작점
```

관련 템플릿은 `src/main/resources/templates` 아래에 있습니다.

---

## 6. 데이터 처리 파이프라인

이 프로젝트의 데이터 처리는 크게 아래 단계로 이루어집니다.

1. KOBIS raw 적재
2. KOBIS 정규화
3. TMDB raw 적재
4. TMDB 정규화
5. 추천용 태그 생성
6. 사용자 활동 데이터 결합
7. 추천 프로필 / 랭킹 / 블록 생성

### 6.1 KOBIS raw 적재

KOBIS 원본 응답은 raw 테이블에 먼저 저장합니다.

주요 raw 테이블:

- `kobis_movie_raw`

이 단계에서는 API 응답을 그대로 저장하고, 이후 정규화 단계에서 서비스용 구조로 분리합니다.

### 6.2 KOBIS 정규화

핵심 파일:

- `KobisMovieNormalizeService.java`

역할:

- KOBIS를 국내 영화 데이터의 기본 골격으로 사용
- `movie` 본체 생성/업데이트
- `movie_source`에 KOBIS 출처 연결
- 장르/감독/배우/제작사/심의 정보를 관계형 테이블로 분리

정규화 대상 예시:

- `movie`
- `movie_genre`
- `movie_director`
- `movie_actor`
- `movie_company`
- `movie_audit`

KOBIS 정규화는 “국내 영화 기본 구조를 만든다”는 역할에 가깝습니다.

### 6.3 TMDB raw 적재

TMDB 원본 응답도 별도 raw 테이블에 저장합니다.

주요 raw 테이블:

- `tmdb_movie_raw`

이 단계에서 이미 존재하는 영화와 매칭 가능한 TMDB 데이터를 받아옵니다.

### 6.4 TMDB 정규화

핵심 파일:

- `TmdbMovieNormalizeService.java`

역할:

- KOBIS가 만든 영화 골격에 상세 메타데이터를 보강
- 포스터 / 줄거리 / 평점 / 인기도 / 키워드 / OTT 제공처 같은 정보 반영
- `movie_source`에 TMDB 출처 연결

정규화 대상 예시:

- `movie`의 상세 컬럼 보강
- `movie_keyword`
- `keyword`
- `movie_provider`
- `provider`

즉 KOBIS는 기본 골격, TMDB는 상세 메타데이터 보강이라는 역할 분담입니다.

---

## 7. 정규화 방식 상세 설명

이 프로젝트는 “영화 한 줄 테이블” 방식이 아니라, 추천에 바로 사용할 수 있도록 메타데이터를 분리합니다.

### 7.1 정규화 대상

- 영화 본체: `movie`
- 출처 연결: `movie_source`
- 장르: `genre`, `movie_genre`
- 인물: `person`, `movie_director`, `movie_actor`
- 제작사: `company`, `movie_company`
- 심의: `audit`, `movie_audit`
- 키워드: `keyword`, `movie_keyword`
- OTT 제공처: `provider`, `movie_provider`

### 7.2 왜 정규화가 필요한가

추천 알고리즘은 영화를 통째로 비교하는 것이 아니라, 영화의 속성을 feature 단위로 사용합니다.

예를 들어 사용자의 취향은 아래처럼 쪼개어 계산됩니다.

- 어떤 장르를 자주 선택하는가
- 어떤 감독/배우가 반복되는가
- 어떤 키워드가 많이 나타나는가
- 어떤 OTT 제공처가 겹치는가
- 어떤 태그 분위기가 누적되는가

따라서 정규화는 단순한 DB 설계 문제가 아니라, 추천 feature를 만들기 위한 전처리 단계입니다.

### 7.3 source 통합 전략

한 영화가 KOBIS와 TMDB를 모두 가질 수 있도록 `movie_source`를 둡니다.

의도:

- 동일 영화를 출처별로 연결
- 중복 영화 row 생성을 줄임
- 한쪽 출처의 강점은 유지하면서 다른 출처 정보로 보강 가능

---

## 8. 태깅 시스템

태깅은 이 프로젝트 추천 구조의 핵심입니다.

장르만으로는 추천 품질이 쉽게 퍼지기 때문에, 별도의 추천용 태그 레이어를 만듭니다.

핵심 파일:

- `MovieTagService.java`
- `MovieTagGenerator.java`
- `DefaultRecommendationTagRules.java`
- `TagScoreCalculator.java`
- `RecommendationTag.java`
- `MovieTagInput.java`

### 8.1 태깅의 목적

장르만으로는 다음 문제가 있습니다.

- `드라마`, `모험`, `액션`처럼 범위가 너무 넓다
- 사용자 취향의 “분위기”를 충분히 설명하기 어렵다
- 추천 이유를 구체적으로 설명하기 어렵다

그래서 아래 같은 태그를 별도로 생성합니다.

- `tense`
- `mystery`
- `dark`
- `healing`
- `romantic`
- `with_family`
- `with_partner`
- `investigation`
- `coming_of_age`
- `true_story`
- `survival`
- `revenge`

### 8.2 태깅 입력값

`MovieTagInput`에는 영화 1편에 대해 다음 정보를 넣습니다.

- 영화 ID
- 제목
- 상영시간
- 개봉연도
- 장르 집합
- 키워드 집합

### 8.3 태깅 후보 필터링

`MovieTagService`는 모든 영화를 태그 후보로 쓰지 않습니다.

대표적인 후보 필터:

- 성인 영화 제외
- 장르가 없는 영화 제외
- 공연/콘서트 계열 제외
- 제목/키워드 기준으로 추천과 맞지 않는 항목 제외

즉 태그 품질을 위해 후보 자체를 한 번 정제합니다.

### 8.4 태그 점수 계산 방식

`TagScoreCalculator`는 각 태그 규칙에 대해 점수를 계산합니다.

구성 요소 예시:

- 장르 포함 여부
- 키워드 포함 여부
- runtime 조건
- year 조건
- positive / negative / exclude 조건

각 태그 규칙은 threshold를 넘었을 때만 실제 태그로 채택됩니다.

### 8.5 태그 선택 방식

`MovieTagGenerator`는 다음 순서로 태그를 고릅니다.

1. 모든 규칙 점수 계산
2. threshold를 넘은 태그만 후보로 선택
3. 태그 타입별 제한 적용
4. 전체 태그 수 제한 적용

현재 제한:

- 타입별 제한 존재
- 전체 태그 수 제한 존재

즉 점수가 높다고 해서 한 영화에 태그를 무한정 붙이지 않습니다.

### 8.6 태그 저장

생성된 결과는 아래 테이블에 저장됩니다.

- `tag`
- `movie_tag`

`tag`는 마스터 테이블이고, `movie_tag`가 영화와 태그의 연결을 저장합니다.

---

## 9. 추천 알고리즘 개요

현재 추천은 **규칙 기반 태그 중심 하이브리드 콘텐츠 추천**입니다.

핵심 파일:

- `UserPreferenceProfileService.java`
- `RecommendationFeaturePolicy.java`
- `RecommendationRankingService.java`
- `RecommendationBlockService.java`
- `RecommendationMaintenanceService.java`
- `RecommendationRefreshStateService.java`

전체 흐름은 아래와 같습니다.

1. 사용자 활동 수집
2. 사용자 취향 프로필 생성
3. 전체 개인화 랭킹 생성
4. 추천 블록 생성

즉 카테고리마다 별도 추천 엔진을 만들지 않고, 먼저 전체 랭킹 1개를 만들고 이를 재사용합니다.

---

## 10. 사용자 신호와 가중치

현재 추천에 반영되는 사용자 신호는 다음과 같습니다.

- 인생영화
- 좋아요
- 봤어요
- 나중에 볼래요

`RecommendationFeaturePolicy` 기준 기본 가중치는 아래와 같습니다.

- 인생영화: `5.0`
- 좋아요: `3.0`
- 봤어요: 기본 `2.2`
- 나중에 볼래요: `1.5`

### 봤어요 + 별점 보정

`WATCHED` 상태는 별점으로 강도가 보정됩니다.

- 별점 없음: `2.2 * 0.70`
- 1점: `2.2 * 0.15`
- 2점: `2.2 * 0.45`
- 3점: `2.2 * 0.85`
- 4점: `2.2 * 1.05`
- 5점: `2.2 * 1.20`

의도:

- 인생영화 > 좋아요 > 봤어요 > 나중에 볼래요
- watched는 약~중간 positive signal
- 높은 별점일수록 stronger positive
- 낮은 별점은 영향이 약함

현재는 `WATCHED`만 추천에 직접 반영하고, `WATCHING`은 아직 강한 추천 신호로 사용하지 않습니다.

---

## 11. 취향 프로필 생성 방식

핵심 파일:

- `UserPreferenceProfileService.java`

사용자 행동을 그대로 쓰지 않고, 그 영화들에 공통으로 나타나는 feature를 누적해서 `user_preference_profile`을 만듭니다.

### 11.1 저장되는 feature 타입

- `TAG`
- `CAUTION`
- `GENRE`
- `DIRECTOR`
- `ACTOR`
- `KEYWORD`
- `PROVIDER`

### 11.2 생성 원리

예를 들어 사용자가 좋아요 / 인생영화 / 봤어요를 누른 영화들에서

- 어떤 태그가 많이 나왔는지
- 어떤 장르가 자주 반복되는지
- 어떤 감독/배우가 겹치는지
- 어떤 키워드/OTT가 반복되는지

를 누적 점수화합니다.

즉 “좋아한 영화 목록”을 그대로 쓰는 게 아니라, 그 안에서 **공통된 취향 패턴을 추출한 프로필**을 만드는 구조입니다.

### 11.3 노이즈 줄이기 정책

현재 프로필 생성 시 아래 정리 정책이 들어가 있습니다.

- 배우: 상위 출연진만 사용 (`display_order <= 5`)
- 키워드: 상위 일부만 사용 (`display_order <= 8`)
- 키워드 blacklist 적용
  - `sequel`
  - `duringcreditsstinger`
  - `based on novel or book`
  - `spin off`
  - 등
- OTT: `KR + FLATRATE`만 사용
- broad genre 감쇠
  - 예: 모험, 드라마, 액션, 코미디

---

## 12. 전체 개인화 랭킹 생성 방식

핵심 파일:

- `RecommendationRankingService.java`

### 12.1 후보 생성

모든 영화를 무작정 점수화하지 않고, 프로필 feature와 하나라도 겹치는 영화만 후보로 잡습니다.

### 12.2 추천 후보 제외

이미 사용자가 반응한 영화는 추천 후보에서 제외합니다.

- 인생영화
- 좋아요한 영화
- 나중에 볼래요 한 영화
- 봤어요(`WATCHED`) 처리한 영화

### 12.3 세부 점수

후보마다 다음 점수를 계산합니다.

- `tag_score`
- `genre_score`
- `people_score`
- `keyword_score`
- `provider_score`
- `penalty_score`
- `popularity_score`
- `freshness_bonus`

`people_score`는 감독과 배우 점수를 합친 값입니다.

### 12.4 최종 가중치

현재 정책값:

- TAG: `0.34`
- GENRE: `0.18`
- PEOPLE: `0.16`
- KEYWORD: `0.08`
- PROVIDER: `0.02`
- POPULARITY: `0.10`
- FRESHNESS: `0.04`
- multi-signal bonus: 최대 `0.05`
- caution penalty: 별도 차감

즉 우선순위는 다음과 같습니다.

1. 태그
2. 장르
3. 감독 / 배우
4. 키워드
5. OTT
6. 인기도 / 최신성 보정

### 12.5 태그 과증폭 보정

예전처럼 태그 1개만 맞아도 점수가 과하게 오르지 않도록 정규화를 넣었습니다.

`computeMatch()`는 다음 요소를 함께 봅니다.

- 프로필 전체 대비 매칭 비율
- 매칭된 feature 개수
- 후보 영화 안에서의 feature 밀도

즉 “1개만 겨우 맞은 영화”와 “여러 feature가 실제로 겹치는 영화”를 구분하려는 구조입니다.

### 12.6 diversity 재정렬

최종 정렬 전 상위 후보에 간단한 diversity 보정을 넣습니다.

최근 추천과

- 같은 대표 태그
- 같은 대표 장르
- 같은 대표 감독

이 연속되면 약간의 penalty를 줍니다.

복잡한 MMR이 아니라, 유지보수 가능한 단순 rerank 방식입니다.

### 12.7 추천 이유 생성

추천 결과에는 `reason_summary`도 저장합니다.

예시:

- 긴장감 있는 태그 일치
- 미스터리 장르 + 특정 감독
- 감성 태그 + 로맨스 장르

즉 “왜 추천되었는지”를 설명 가능한 구조로 유지합니다.

---

## 13. 추천 블록 생성 방식

핵심 파일:

- `RecommendationBlockService.java`

추천 블록은 별도 추천 엔진이 아닙니다.

방식:

1. 전체 랭킹 상위 slice를 가져옴
2. 그 안에서 특정 feature를 기준으로 다시 묶음
3. 블록 최소 크기를 만족하는 경우만 노출

현재 블록 후보:

- 개인화 추천
- TAG 기반 블록
- GENRE 기반 블록
- DIRECTOR 기반 블록
- ACTOR 기반 블록
- PROVIDER 기반 블록

정책:

- 상위 `slice`만 사용해 tail noise 감소
- 샘플이 너무 적으면 블록 생성 안 함
- broad genre는 더 엄격하게 처리

즉 블록은 “랭킹 결과를 보기 좋게 재구성한 뷰”입니다.

---

## 14. 추천 갱신 방식

현재 추천은 **dirty 후 지연 재계산** 구조입니다.

관련 파일:

- `RecommendationRefreshStateService.java`
- `RecommendationMaintenanceService.java`

동작:

- 사용자가 활동을 남기면 `dirty = true`
- 즉시 무거운 전체 재계산은 하지 않음
- 이후 홈 진입 / 관리자 배치 시 재계산

장점:

- 클릭 즉시 응답이 가벼움
- 추천 계산은 필요한 시점에만 수행

---

## 15. 관리자 배치 / 시드 / 검증

관련 패키지:

- `com.cinematch.admin`

주요 역할:

- 영화 데이터 적재
- 정규화 배치
- 태그 재생성
- 더미 유저 생성
- 더미 활동 시드
- 추천 재계산
- 추천 검증

즉 개발/시연 단계에서 데이터와 추천 상태를 재현할 수 있도록 관리자 배치 흐름을 갖추고 있습니다.

---

## 16. 현재 구현 완료 범위

### 완료

- KOBIS raw import
- KOBIS normalize
- TMDB import / normalize
- 영화 메타데이터 정규화
- 규칙 기반 태그 생성
- 사용자 활동 기반 취향 프로필 생성
- 전체 개인화 랭킹 생성
- 랭킹 기반 추천 블록 생성
- 상세 페이지 액션 시스템
- watched + rating 저장 및 반영
- 더미 유저 / 더미 활동 데이터
- 팔로우 기반 소셜 UI

### 진행 중 / 보완 가능

- 추천 품질 세부 튜닝
- 신규 신호(dislike, collection, social signal) 추천 반영 여부
- 회원가입 시 초기 선호 입력
- UI 최종 다듬기
- 평가 지표와 자동 검증 강화

---

## 17. 참고 파일

정규화:

- `src/main/java/com/cinematch/kobis/KobisMovieNormalizeService.java`
- `src/main/java/com/cinematch/tmdb/TmdbMovieNormalizeService.java`

태깅:

- `src/main/java/com/cinematch/tag/MovieTagService.java`
- `src/main/java/com/cinematch/tag/MovieTagGenerator.java`
- `src/main/java/com/cinematch/tag/DefaultRecommendationTagRules.java`
- `src/main/java/com/cinematch/tag/TagScoreCalculator.java`

추천:

- `src/main/java/com/cinematch/recommendation/UserPreferenceProfileService.java`
- `src/main/java/com/cinematch/recommendation/RecommendationFeaturePolicy.java`
- `src/main/java/com/cinematch/recommendation/RecommendationRankingService.java`
- `src/main/java/com/cinematch/recommendation/RecommendationBlockService.java`
- `src/main/java/com/cinematch/recommendation/RecommendationMaintenanceService.java`

관리 배치:

- `src/main/java/com/cinematch/admin/AdminBatchController.java`
- `src/main/java/com/cinematch/admin/MovieDataBatchService.java`
- `src/main/java/com/cinematch/admin/DummyUserSeedService.java`
- `src/main/java/com/cinematch/admin/DummyUserActivitySeedService.java`

웹 진입점:

- `src/main/java/com/cinematch/MovieController.java`

---

## 18. 한 줄 요약

CineMatch는 **영화 메타데이터 정규화 + 규칙 기반 태깅 + 사용자 활동 기반 취향 프로필 + 전체 개인화 랭킹 + 랭킹 기반 추천 블록**까지 연결된 영화 추천 웹 프로젝트입니다.
