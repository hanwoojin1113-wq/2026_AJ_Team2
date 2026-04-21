# CineMatch — 변경 이력 ()

---

## 랭킹 페이지 대시보드 개편 (`ranking.html`)

### 변경 배경

기존 랭킹 페이지는 차트 목록을 카드로 나열하고 클릭 시 해당 차트로 이동하는 구조였음.
사용자가 실제 순위 데이터를 보기까지 단계가 많다는 피드백을 반영해 개편.

### 주요 변경 사항

**레이아웃 구조 전환**

- Before: 차트 카드 그리드 (클릭 → 차트 상세 페이지)
- After: 섹션형 대시보드 (각 차트의 Top N 순위를 메인에서 바로 확인)

**Thymeleaf 데이터 모델 변경**

- Before: `${entries}` — 차트 메타 정보 목록
- After: `${sections}` — 차트 메타(`entry()`) + 순위 데이터(`rows()`)를 함께 보유

**새로 추가된 UI 요소**

- `rank-row` — 순위번호 / 포스터 / 영화정보 / 지표값 4컬럼 그리드
- `rank-poster-wrap` — 포스터 이미지, fallback 텍스트, 뱃지 오버레이 포함
- `rank-metric` — 지표 라벨 + 수치 표시
- `see-all-btn` — "전체 보기" 버튼 (기존 "순위 보기" CTA 대체)
- `section-empty` — 데이터 없을 때 빈 상태 메시지

**기타 수치 조정**

- 페이지 최대 너비: `1440px` → `1200px`
- 모바일에서 지표 컬럼(`rank-metric`) 자동 숨김

---

## 감독 차트 중복 출력 버그 수정 (`DirectorChart.java`)

### 문제

공동 감독 영화가 감독 수만큼 중복으로 순위에 출력되는 버그.

### 원인

`movie_director` 테이블 조인 시 영화-감독 N:M 관계에서 중복 행이 발생했고,
이를 걸러내는 로직이 없었음.

### 해결

서브쿼리로 감싼 뒤 `ROW_NUMBER() OVER (PARTITION BY m.id)` 를 적용,
영화 ID 기준으로 중복을 제거해 영화당 1행만 출력되도록 수정.

```sql
ROW_NUMBER() OVER (
    PARTITION BY m.id
    ORDER BY dir_totals.total_audi DESC
) AS movie_rn
...
WHERE movie_rn = 1
```

정렬 기준도 `box_office_audi_acc DESC` → `movie_code` 로 변경
(중복 제거 이후 안정적인 정렬 보장).
