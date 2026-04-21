package com.cinematch.tag;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
public class MovieTagService {
    /*
     * 태그 계층은 장르/키워드만으로 설명하기 어려운 "추천용 분위기 feature"를 만드는 단계다.
     * 예를 들어 tense, healing, with_family 같은 태그를 movie_tag로 저장해두면
     * 추천 알고리즘이 장르보다 더 세밀한 취향 패턴을 읽을 수 있다.
     */

    private static final String TITLE_PATTERN_CONCERT_KR = "%\uCF58\uC11C\uD2B8%";
    private static final String TITLE_PATTERN_LIVE_KR = "%\uB77C\uC774\uBE0C%";
    private static final String TITLE_PATTERN_WORLD_TOUR_KR = "%\uC6D4\uB4DC \uD22C\uC5B4%";
    private static final String TITLE_PATTERN_FAN_CONCERT_KR = "%\uD32C \uCF58\uC11C\uD2B8%";
    private static final String TITLE_PATTERN_LISTENING_KR = "%\uCCAD\uC74C\uD68C%";
    private static final String TITLE_PATTERN_IN_CINEMA_KR = "%\uC778 \uC2DC\uB124\uB9C8%";
    private static final String TITLE_PATTERN_PERFORMANCE_KR = "%\uACF5\uC5F0%";
    private static final String TITLE_PATTERN_CONCERT_EN = "%concert%";
    private static final String TITLE_PATTERN_LIVE_EN = "%live%";
    private static final String TITLE_PATTERN_WORLD_TOUR_EN = "%world tour%";
    private static final String TITLE_PATTERN_FAN_CONCERT_EN = "%fan concert%";
    private static final String TITLE_PATTERN_CINEMA_EN = "%cinema%";

    private static final String CANDIDATE_WHERE = """
            WHERE (m.adult IS NULL OR m.adult = FALSE)
              AND COALESCE(m.title, m.movie_name) IS NOT NULL
              AND NOT EXISTS (
                    SELECT 1
                    FROM movie_genre mg
                    JOIN genre g ON g.id = mg.genre_id
                    WHERE mg.movie_id = m.id
                      AND g.name IN ('%s', '%s')
              )
              AND NOT (
                    LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
                 OR LOWER(COALESCE(m.title, m.movie_name, '')) LIKE '%s'
              )
              AND EXISTS (
                    SELECT 1
                    FROM movie_genre mg
                    WHERE mg.movie_id = m.id
              )
            """.formatted(
            GenreNames.CONCERT,
            GenreNames.MUSICAL,
            TITLE_PATTERN_CONCERT_KR,
            TITLE_PATTERN_LIVE_KR,
            TITLE_PATTERN_WORLD_TOUR_KR,
            TITLE_PATTERN_FAN_CONCERT_KR,
            TITLE_PATTERN_LISTENING_KR,
            TITLE_PATTERN_IN_CINEMA_KR,
            TITLE_PATTERN_PERFORMANCE_KR,
            TITLE_PATTERN_CONCERT_EN,
            TITLE_PATTERN_LIVE_EN,
            TITLE_PATTERN_WORLD_TOUR_EN,
            TITLE_PATTERN_FAN_CONCERT_EN,
            TITLE_PATTERN_CINEMA_EN
    );

    private final JdbcTemplate jdbcTemplate;
    private final MovieTagGenerator movieTagGenerator;
    private static final Set<String> PERFORMANCE_KEYWORDS = Set.of(
            "concert",
            "concert film",
            "live performance",
            "live concert",
            "concert movie"
    );

    public MovieTagService(JdbcTemplate jdbcTemplate, MovieTagGenerator movieTagGenerator) {
        this.jdbcTemplate = jdbcTemplate;
        this.movieTagGenerator = movieTagGenerator;
    }

    @Transactional
    public RebuildResult rebuildTags() {
        // 태그는 파생 데이터라서 항상 "전체 재생성" 전략을 써도 관리가 비교적 단순하다.
        initializeTagTables();
        seedTags();

        List<MovieTagInput> candidates = loadCandidateMovies();
        jdbcTemplate.update("DELETE FROM movie_tag");

        Map<String, Integer> insertedCounts = new LinkedHashMap<>();
        Timestamp createdAt = Timestamp.valueOf(LocalDateTime.now());

        for (MovieTagInput candidate : candidates) {
            // 후보 영화 1편에 대해 규칙 기반 태그 생성기를 돌리고 결과를 movie_tag에 적재한다.
            List<MovieTagResult> generatedTags = movieTagGenerator.generate(candidate);
            for (MovieTagResult generatedTag : generatedTags) {
                Long tagId = findTagId(generatedTag.tagType().name(), generatedTag.tagName());
                if (tagId == null) {
                    continue;
                }

                jdbcTemplate.update("""
                        INSERT INTO movie_tag (movie_id, tag_id, created_at)
                        VALUES (?, ?, ?)
                        """, candidate.movieId(), tagId, createdAt);
                insertedCounts.merge(generatedTag.tagType().name() + ":" + generatedTag.tagName(), 1, Integer::sum);
            }
        }

        int totalTaggedRows = insertedCounts.values().stream().mapToInt(Integer::intValue).sum();
        return new RebuildResult(candidates.size(), totalTaggedRows, insertedCounts);
    }

    public TagStats stats() {
        List<MovieTagInput> candidates = loadCandidateMovies();
        Set<Long> candidateMovieIds = new LinkedHashSet<>();
        for (MovieTagInput candidate : candidates) {
            candidateMovieIds.add(candidate.movieId());
        }

        int movieCount = queryInt("SELECT COUNT(*) FROM movie");
        int keywordMovieCount = queryInt("SELECT COUNT(DISTINCT movie_id) FROM movie_keyword");
        int keywordRowCount = queryInt("SELECT COUNT(*) FROM movie_keyword");
        int taggedMovieCount = queryInt("SELECT COUNT(DISTINCT movie_id) FROM movie_tag");
        int movieTagRowCount = queryInt("SELECT COUNT(*) FROM movie_tag");
        int untaggedKeywordedMovieCount = queryInt("""
                SELECT COUNT(*)
                FROM movie m
                WHERE EXISTS (
                    SELECT 1
                    FROM movie_keyword mk
                    WHERE mk.movie_id = m.id
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM movie_tag mt
                    WHERE mt.movie_id = m.id
                )
                """);

        List<TagCount> tagDistribution = jdbcTemplate.query("""
                SELECT t.tag_type, t.tag_name, COUNT(*) AS movie_count
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                GROUP BY t.tag_type, t.tag_name
                ORDER BY t.tag_type, movie_count DESC, t.tag_name
                """, (rs, rowNum) -> new TagCount(
                rs.getString("tag_type"),
                rs.getString("tag_name"),
                rs.getInt("movie_count")
        ));

        Map<Long, Set<String>> genresByMovie = loadGenresByMovie();
        Set<Long> taggedMovieIds = new HashSet<>(jdbcTemplate.query("""
                SELECT DISTINCT movie_id
                FROM movie_tag
                """, (rs, rowNum) -> rs.getLong("movie_id")));

        Map<String, Integer> totalByGenre = new LinkedHashMap<>();
        Map<String, Integer> taggedByGenre = new LinkedHashMap<>();
        Map<String, Integer> untaggedKeywordedByGenre = new LinkedHashMap<>();

        Set<Long> keywordedMovieIds = new HashSet<>(jdbcTemplate.query("""
                SELECT DISTINCT movie_id
                FROM movie_keyword
                """, (rs, rowNum) -> rs.getLong("movie_id")));

        for (Long movieId : candidateMovieIds) {
            Set<String> genres = genresByMovie.getOrDefault(movieId, Collections.emptySet());
            boolean tagged = taggedMovieIds.contains(movieId);
            boolean keyworded = keywordedMovieIds.contains(movieId);
            for (String genre : genres) {
                totalByGenre.merge(genre, 1, Integer::sum);
                if (tagged) {
                    taggedByGenre.merge(genre, 1, Integer::sum);
                }
                if (keyworded && !tagged) {
                    untaggedKeywordedByGenre.merge(genre, 1, Integer::sum);
                }
            }
        }

        List<GenreCoverage> genreCoverage = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : totalByGenre.entrySet()) {
            String genre = entry.getKey();
            int total = entry.getValue();
            int tagged = taggedByGenre.getOrDefault(genre, 0);
            genreCoverage.add(new GenreCoverage(genre, total, tagged, ratio(tagged, total)));
        }
        genreCoverage.sort(Comparator.comparingInt(GenreCoverage::candidateMovieCount).reversed()
                .thenComparing(GenreCoverage::genreName));

        List<GenreCount> untaggedKeywordedGenreDistribution = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : untaggedKeywordedByGenre.entrySet()) {
            untaggedKeywordedGenreDistribution.add(new GenreCount(entry.getKey(), entry.getValue()));
        }
        untaggedKeywordedGenreDistribution.sort(Comparator.comparingInt(GenreCount::movieCount).reversed()
                .thenComparing(GenreCount::genreName));

        return new TagStats(
                movieCount,
                candidates.size(),
                keywordMovieCount,
                keywordRowCount,
                taggedMovieCount,
                movieTagRowCount,
                untaggedKeywordedMovieCount,
                ratio(movieTagRowCount, Math.max(1, taggedMovieCount)),
                ratio(movieTagRowCount, Math.max(1, candidates.size())),
                tagDistribution,
                genreCoverage,
                untaggedKeywordedGenreDistribution
        );
    }

    private List<MovieTagInput> loadCandidateMovies() {
        // 태그는 모든 영화에 무조건 붙이지 않고, 추천 대상으로 쓸 만한 후보만 골라서 생성한다.
        List<CandidateMovieRow> candidateRows = jdbcTemplate.query("""
                SELECT
                    m.id,
                    COALESCE(m.title, m.movie_name) AS display_title,
                    COALESCE(m.release_date, m.movie_info_open_date) AS release_date,
                    COALESCE(m.runtime, m.show_time) AS runtime_minutes
                FROM movie m
                """ + CANDIDATE_WHERE + """
                ORDER BY m.id
                """, (rs, rowNum) -> {
            Date releaseDate = rs.getDate("release_date");
            return new CandidateMovieRow(
                    rs.getLong("id"),
                    rs.getString("display_title"),
                    releaseDate == null ? null : releaseDate.toLocalDate(),
                    (Integer) rs.getObject("runtime_minutes")
            );
        });

        Map<Long, Set<String>> genresByMovie = loadGenresByMovie();
        Map<Long, Set<String>> keywordsByMovie = loadKeywordsByMovie();

        List<MovieTagInput> inputs = new ArrayList<>();
        for (CandidateMovieRow candidateRow : candidateRows) {
            Set<String> genres = genresByMovie.getOrDefault(candidateRow.movieId(), Collections.emptySet());
            Set<String> keywords = keywordsByMovie.getOrDefault(candidateRow.movieId(), Collections.emptySet());
            if (shouldExcludeCandidate(candidateRow.title(), keywords)) {
                continue;
            }
            Integer releaseYear = candidateRow.releaseDate() == null ? null : candidateRow.releaseDate().getYear();
            inputs.add(new MovieTagInput(
                    candidateRow.movieId(),
                    candidateRow.title(),
                    candidateRow.runtimeMinutes(),
                    releaseYear,
                    genres,
                    keywords
            ));
        }
        return inputs;
    }

    private boolean shouldExcludeCandidate(String title, Set<String> keywords) {
        // 공연/콘서트 계열처럼 추천 톤과 맞지 않는 항목은 후보 단계에서 제외한다.
        if (title == null || title.isBlank()) {
            return true;
        }

        String normalizedTitle = title.toLowerCase(Locale.ROOT);
        if (normalizedTitle.contains("\uACF5\uC5F0")) {
            return true;
        }
        if (hasUnsupportedPrimaryScript(title)) {
            return true;
        }
        return !Collections.disjoint(keywords, PERFORMANCE_KEYWORDS);
    }

    private boolean hasUnsupportedPrimaryScript(String value) {
        int supportedLetterCount = 0;
        int unsupportedLetterCount = 0;

        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (!Character.isLetter(ch)) {
                continue;
            }

            if (isHangul(ch) || isLatin(ch)) {
                supportedLetterCount++;
            } else {
                unsupportedLetterCount++;
            }
        }

        if (unsupportedLetterCount == 0) {
            return false;
        }
        if (supportedLetterCount == 0) {
            return true;
        }
        return unsupportedLetterCount > supportedLetterCount;
    }

    private boolean isHangul(char ch) {
        return (ch >= '\uAC00' && ch <= '\uD7A3')
                || (ch >= '\u1100' && ch <= '\u11FF')
                || (ch >= '\u3130' && ch <= '\u318F');
    }

    private boolean isLatin(char ch) {
        return (ch >= 'A' && ch <= 'Z')
                || (ch >= 'a' && ch <= 'z')
                || (ch >= '\u00C0' && ch <= '\u024F');
    }

    private Map<Long, Set<String>> loadGenresByMovie() {
        // 태그 생성 입력은 movie 본체가 아니라 정규화된 genre/keyword 관계를 재사용한다.
        Map<Long, Set<String>> genresByMovie = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT mg.movie_id, g.name
                FROM movie_genre mg
                JOIN genre g ON g.id = mg.genre_id
                JOIN movie m ON m.id = mg.movie_id
                """ + CANDIDATE_WHERE, rs -> {
            long movieId = rs.getLong("movie_id");
            genresByMovie.computeIfAbsent(movieId, ignored -> new LinkedHashSet<>())
                    .add(rs.getString("name"));
        });
        return toUnmodifiableSetMap(genresByMovie);
    }

    private Map<Long, Set<String>> loadKeywordsByMovie() {
        Map<Long, Set<String>> keywordsByMovie = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT mk.movie_id, k.name
                FROM movie_keyword mk
                JOIN keyword k ON k.id = mk.keyword_id
                JOIN movie m ON m.id = mk.movie_id
                """ + CANDIDATE_WHERE, rs -> {
            long movieId = rs.getLong("movie_id");
            String keyword = rs.getString("name");
            if (keyword == null || keyword.isBlank()) {
                return;
            }

            keywordsByMovie.computeIfAbsent(movieId, ignored -> new LinkedHashSet<>())
                    .add(keyword.toLowerCase(Locale.ROOT).trim());
        });
        return toUnmodifiableSetMap(keywordsByMovie);
    }

    private Map<Long, Set<String>> toUnmodifiableSetMap(Map<Long, Set<String>> source) {
        Map<Long, Set<String>> normalized = new LinkedHashMap<>();
        for (Map.Entry<Long, Set<String>> entry : source.entrySet()) {
            normalized.put(entry.getKey(), Collections.unmodifiableSet(new LinkedHashSet<>(entry.getValue())));
        }
        return normalized;
    }

    private Long findTagId(String tagType, String tagName) {
        return jdbcTemplate.query("""
                SELECT id
                FROM tag
                WHERE tag_type = ? AND tag_name = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, tagType, tagName);
    }

    private int queryInt(String sql) {
        Integer value = jdbcTemplate.queryForObject(sql, Integer.class);
        return value == null ? 0 : value;
    }

    private double ratio(int numerator, int denominator) {
        if (denominator <= 0) {
            return 0.0;
        }
        return Math.round((numerator * 1000.0) / denominator) / 1000.0;
    }

    private void seedTags() {
        // RecommendationTag enum을 기준으로 마스터 tag 테이블을 먼저 맞춰둔다.
        for (RecommendationTag recommendationTag : RecommendationTag.values()) {
            seedTag(recommendationTag.type().name(), recommendationTag.code());
        }
    }

    private void seedTag(String tagType, String tagName) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM tag
                WHERE tag_type = ? AND tag_name = ?
                """, Integer.class, tagType, tagName);

        if (count != null && count > 0) {
            return;
        }

        jdbcTemplate.update("""
                INSERT INTO tag (tag_name, tag_type)
                VALUES (?, ?)
                """, tagName, tagType);
    }

    private void initializeTagTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS tag (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    tag_name VARCHAR(100) NOT NULL,
                    tag_type VARCHAR(20) NOT NULL,
                    CONSTRAINT uk_tag_name_type UNIQUE (tag_name, tag_type)
                )
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS movie_tag (
                    movie_id BIGINT NOT NULL,
                    tag_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (movie_id, tag_id),
                    CONSTRAINT fk_movie_tag_movie FOREIGN KEY (movie_id) REFERENCES movie(id),
                    CONSTRAINT fk_movie_tag_tag FOREIGN KEY (tag_id) REFERENCES tag(id)
                )
                """);
    }

    private record CandidateMovieRow(
            long movieId,
            String title,
            LocalDate releaseDate,
            Integer runtimeMinutes
    ) {
    }

    public record RebuildResult(
            int candidateCount,
            int totalTaggedRows,
            Map<String, Integer> insertedCounts
    ) {
    }

    public record TagStats(
            int movieCount,
            int candidateCount,
            int keywordMovieCount,
            int keywordRowCount,
            int taggedMovieCount,
            int movieTagRowCount,
            int untaggedKeywordedMovieCount,
            double averageTagsPerTaggedMovie,
            double averageTagsPerCandidateMovie,
            List<TagCount> tagDistribution,
            List<GenreCoverage> genreCoverage,
            List<GenreCount> untaggedKeywordedGenreDistribution
    ) {
    }

    public record TagCount(
            String tagType,
            String tagName,
            int movieCount
    ) {
    }

    public record GenreCoverage(
            String genreName,
            int candidateMovieCount,
            int taggedMovieCount,
            double coverageRate
    ) {
    }

    public record GenreCount(
            String genreName,
            int movieCount
    ) {
    }
}
