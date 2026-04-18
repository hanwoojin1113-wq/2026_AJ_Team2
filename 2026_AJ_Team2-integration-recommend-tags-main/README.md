#

추가한 기능에 대한 README입니다 !
또, templates 파일들을 밝은 버전 + 효과 버전으로 만들어봤는데, 기능상에 오류가 있어서 추후에 오류 수정해서 올리겠습니다 !

# 랭킹 차트 시스템

박스오피스 데이터 기반 12가지 알고리즘 차트 시스템입니다.

---

## 파일 위치

```
src/main/java/movie/web/login/chart/
├── ChartAlgorithm.java
├── ChartCategory.java
├── ChartMovieRow.java
├── ChartEntry.java
├── AbstractJdbcChartAlgorithm.java
├── ChartRegistry.java
├── ChartController.java
└── algorithms/
    ├── TopSalesChart.java
    ├── MillionClubChart.java
    ├── EfficientChart.java
    ├── UnderscreenChart.java
    ├── AudiPerShowChart.java
    ├── PremiumChart.java
    ├── FlashHitChart.java
    ├── LongRunnerChart.java
    ├── ClassicChart.java
    ├── DecadeChart.java
    ├── GenreTopChart.java
    └── DirectorChart.java

src/main/resources/templates/
├── ranking.html
└── ranking-detail.html
```

---

## 화면 구성

- `GET /ranking` → 차트 목록 페이지. 6개 카테고리 탭으로 필터링 가능
- `GET /ranking/{code}` → 특정 차트의 순위 리스트 페이지

---

## 클래스 설명

### ChartAlgorithm

모든 알고리즘 클래스가 구현하는 인터페이스입니다.
`code()`, `title()`, `description()`, `category()`, `icon()`, `fetch(int limit)` 메서드를 구현해야 합니다.

### ChartCategory

차트 카테고리를 정의하는 enum입니다.
`BOXOFFICE`, `AUDIENCE`, `EFFICIENCY`, `TIME`, `GENRE`, `PEOPLE` 6가지입니다.

### ChartMovieRow

순위 결과 한 행을 담는 record DTO입니다.
모든 알고리즘이 같은 타입을 반환하므로 템플릿 하나로 12개 차트를 모두 렌더링할 수 있습니다.

### ChartEntry

랭킹 목록 페이지에서 카드 형태로 표시되는 메타데이터 DTO입니다.
실제 영화 데이터는 포함하지 않으며, `ChartAlgorithm`으로부터 생성됩니다.

### AbstractJdbcChartAlgorithm

`JdbcTemplate` 주입과 공통 쿼리 실행 로직을 제공하는 베이스 클래스입니다.
하위 알고리즘 클래스는 SQL만 작성하면 됩니다.

### ChartRegistry

`@Component`로 등록된 모든 `ChartAlgorithm` 구현체를 자동으로 수집해 관리하는 등록소입니다.
새 알고리즘 클래스에 `@Component`를 달면 자동으로 등록됩니다.

### ChartController

`/ranking`, `/ranking/{code}` 두 엔드포인트를 처리합니다.
두 메서드 모두 세션에 `loginUserId`가 없으면 `/login`으로 리다이렉트합니다.

---

## 알고리즘 목록

| 코드            | 이름                  | 카테고리   | 정렬 기준                                |
| --------------- | --------------------- | ---------- | ---------------------------------------- |
| `top-sales`     | 역대 매출 순위        | 박스오피스 | `box_office_sales_acc DESC`              |
| `million-club`  | 천만 클럽             | 관객       | `box_office_audi_acc >= 10,000,000`      |
| `efficient`     | 가성비 영화           | 효율       | `audi_acc / scrn_cnt DESC`               |
| `underscreen`   | 스크린 독점 없이 흥행 | 효율       | 스크린 수 중앙값 이하 중 `audi_acc DESC` |
| `audi-per-show` | 회차당 관객 순위      | 효율       | `audi_acc / show_cnt DESC`               |
| `premium`       | 특별관 선호 영화      | 효율       | `sales_acc / audi_acc DESC`              |
| `flash-hit`     | 반짝 흥행 영화        | 시대       | `sales_acc / 개봉 후 경과일 DESC`        |
| `long-runner`   | 오래 사랑받은 영화    | 시대       | `show_cnt / scrn_cnt DESC`               |
| `classic`       | 클래식 재발견         | 시대       | 개봉 10년 이상 중 `audi_acc DESC`        |
| `decade`        | 연대별 흥행왕         | 시대       | 10년 단위 연대별 관객 1위                |
| `genre-top`     | 장르별 역대 1위       | 장르       | 장르별 `audi_acc` 1위                    |
| `director`      | 감독 흥행 보증 수표   | 인물       | 흥행작 2편 이상 감독, 총 관객 합계 DESC  |

---

## 새 차트 추가 방법

`AbstractJdbcChartAlgorithm`을 상속한 클래스를 `algorithms/` 패키지에 만들고 `@Component`를 달면 끝입니다.
`ChartController`나 `ChartRegistry`는 수정하지 않아도 됩니다.

```java
@Component
public class MyNewChart extends AbstractJdbcChartAlgorithm {

    public MyNewChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()            { return "my-new"; }
    @Override public String title()           { return "새 차트"; }
    @Override public String description()     { return "차트 설명"; }
    @Override public ChartCategory category() { return ChartCategory.BOXOFFICE; }
    @Override public String icon()            { return "chart-bar"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        return runQuery("""
                SELECT
                    m.movie_cd                                        AS movie_code,
                    COALESCE(m.title, m.movie_name)                  AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title)      AS movie_name_en,
                    m.poster_image_url,
                    COALESCE(m.release_date, m.box_office_open_date) AS open_date,
                    m.production_year,
                    CAST(m.box_office_sales_acc AS VARCHAR)          AS metric_value
                FROM movie m
                WHERE m.box_office_sales_acc IS NOT NULL
                ORDER BY m.box_office_sales_acc DESC
                """,
                new Object[]{}, limit, "지표 이름", null);
    }
}
```

---

## 기존 파일 수정 내역

### TopSalesChart.java

H2 DB에서 지원하지 않는 `FORMAT()` 함수를 `CAST(... AS VARCHAR)`로 교체했습니다.

### ChartController.java

두 엔드포인트에 로그인 체크를 추가했습니다.

```java
if (session.getAttribute("loginUserId") == null) return "redirect:/login";
```

### index.html

navbar에 랭킹 차트 진입 링크를 추가했습니다.

```html
<a class="nav-tab" th:href="@{/ranking}">랭킹</a>
```
