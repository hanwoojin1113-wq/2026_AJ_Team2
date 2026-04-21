package com.cinematch.kobis;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

@Service
public class KobisMovieImportService {

    private static final int DEFAULT_TARGET_MOVIE_COUNT = 300;
    private static final int MAX_TARGET_MOVIE_COUNT = 1000;
    private static final int DEFAULT_TARGET_PER_YEAR = 30;
    private static final int MAX_TARGET_PER_YEAR = 100;
    private static final DateTimeFormatter KOBIS_DATE = DateTimeFormatter.BASIC_ISO_DATE;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final String baseUrl;
    private final String apiKey;

    public KobisMovieImportService(
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            @Value("${kobis.base-url:https://www.kobis.or.kr/kobisopenapi/webservice/rest}") String baseUrl,
            @Value("${kobis.api-key:}") String apiKey,
            @Value("${kobis.connect-timeout-ms:5000}") int connectTimeoutMs,
            @Value("${kobis.read-timeout-ms:10000}") int readTimeoutMs
    ) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(connectTimeoutMs);
        requestFactory.setReadTimeout(readTimeoutMs);
        this.restTemplate = new RestTemplate(requestFactory);
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
    }

    @Transactional
    public ImportResult importWeeklyBoxOfficeMovies() {
        return importWeeklyBoxOfficeMovies(DEFAULT_TARGET_MOVIE_COUNT);
    }

    @Transactional
    public ImportResult importWeeklyBoxOfficeMovies(int targetMovieCount) {
        requireApiKey();
        initializeRawTable();

        int normalizedTargetMovieCount = Math.min(Math.max(targetMovieCount, 1), MAX_TARGET_MOVIE_COUNT);
        int maxWeeksToScan = Math.max(120, normalizedTargetMovieCount);

        int scannedWeekCount = 0;
        int savedRawCount = 0;
        int updatedRawCount = 0;
        int failedWeekCount = 0;
        int failedMovieInfoCount = 0;
        Set<String> uniqueMovieCodes = new LinkedHashSet<>();
        LocalDate targetDate = LocalDate.now()
                .minusDays(1)
                .with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));

        while (uniqueMovieCodes.size() < normalizedTargetMovieCount && scannedWeekCount < maxWeeksToScan) {
            JsonNode weeklyRoot;
            try {
                weeklyRoot = readTree(restTemplate.getForObject(buildWeeklyBoxOfficeUri(targetDate), String.class));
            } catch (RuntimeException e) {
                failedWeekCount++;
                scannedWeekCount++;
                targetDate = targetDate.minusWeeks(1);
                continue;
            }

            JsonNode weeklyList = weeklyRoot.path("boxOfficeResult").path("weeklyBoxOfficeList");
            if (weeklyList.isArray()) {
                for (JsonNode weeklyEntry : weeklyList) {
                    String movieCd = textValue(weeklyEntry, "movieCd");
                    if (movieCd == null || !uniqueMovieCodes.add(movieCd)) {
                        continue;
                    }

                    JsonNode movieInfo;
                    try {
                        movieInfo = readTree(restTemplate.getForObject(buildMovieInfoUri(movieCd), String.class))
                                .path("movieInfoResult")
                                .path("movieInfo");
                    } catch (RuntimeException e) {
                        uniqueMovieCodes.remove(movieCd);
                        failedMovieInfoCount++;
                        continue;
                    }
                    if (movieInfo.isMissingNode() || movieInfo.isNull()) {
                        uniqueMovieCodes.remove(movieCd);
                        continue;
                    }

                    String payload = buildPayload(weeklyEntry, movieInfo);
                    if (upsertRawMovie(movieCd, payload)) {
                        savedRawCount++;
                    } else {
                        updatedRawCount++;
                    }

                    if (uniqueMovieCodes.size() >= normalizedTargetMovieCount) {
                        break;
                    }
                }
            }

            scannedWeekCount++;
            targetDate = targetDate.minusWeeks(1);
        }

        return new ImportResult(
                uniqueMovieCodes.size(),
                scannedWeekCount,
                savedRawCount,
                updatedRawCount,
                failedWeekCount,
                failedMovieInfoCount
        );
    }

    @Transactional
    public YearlyImportResult importYearlyRepresentativeMovies(Integer startYear, Integer endYear, int targetPerYear) {
        requireApiKey();
        initializeRawTable();

        int currentYear = LocalDate.now().getYear();
        int normalizedEndYear = endYear == null ? currentYear : endYear;
        int normalizedStartYear = startYear == null ? normalizedEndYear - 9 : startYear;
        if (normalizedStartYear > normalizedEndYear) {
            throw new IllegalArgumentException("startYear must be less than or equal to endYear.");
        }

        int normalizedTargetPerYear = Math.min(Math.max(targetPerYear, 1), MAX_TARGET_PER_YEAR);
        int scannedWeekCount = 0;
        int savedRawCount = 0;
        int updatedRawCount = 0;
        int failedWeekCount = 0;
        int failedMovieInfoCount = 0;
        Set<String> allCollectedMovieCodes = new LinkedHashSet<>();
        Map<Integer, Integer> collectedCountsByYear = new LinkedHashMap<>();

        for (int year = normalizedEndYear; year >= normalizedStartYear; year--) {
            Set<String> yearMovieCodes = new LinkedHashSet<>();
            LocalDate targetDate = LocalDate.of(year, 12, 31)
                    .with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
            LocalDate lowerBound = LocalDate.of(year, 1, 1);

            while (yearMovieCodes.size() < normalizedTargetPerYear && !targetDate.isBefore(lowerBound)) {
                JsonNode weeklyRoot;
                try {
                    weeklyRoot = readTree(restTemplate.getForObject(buildWeeklyBoxOfficeUri(targetDate), String.class));
                } catch (RuntimeException e) {
                    failedWeekCount++;
                    scannedWeekCount++;
                    targetDate = targetDate.minusWeeks(1);
                    continue;
                }

                JsonNode weeklyList = weeklyRoot.path("boxOfficeResult").path("weeklyBoxOfficeList");
                if (weeklyList.isArray()) {
                    for (JsonNode weeklyEntry : weeklyList) {
                        String movieCd = textValue(weeklyEntry, "movieCd");
                        if (movieCd == null || yearMovieCodes.contains(movieCd)) {
                            continue;
                        }

                        JsonNode movieInfo;
                        try {
                            movieInfo = readTree(restTemplate.getForObject(buildMovieInfoUri(movieCd), String.class))
                                    .path("movieInfoResult")
                                    .path("movieInfo");
                        } catch (RuntimeException e) {
                            failedMovieInfoCount++;
                            continue;
                        }

                        if (movieInfo.isMissingNode() || movieInfo.isNull()) {
                            continue;
                        }

                        String payload = buildPayload(weeklyEntry, movieInfo);
                        if (upsertRawMovie(movieCd, payload)) {
                            savedRawCount++;
                        } else {
                            updatedRawCount++;
                        }

                        yearMovieCodes.add(movieCd);
                        allCollectedMovieCodes.add(movieCd);
                        if (yearMovieCodes.size() >= normalizedTargetPerYear) {
                            break;
                        }
                    }
                }

                scannedWeekCount++;
                targetDate = targetDate.minusWeeks(1);
            }

            collectedCountsByYear.put(year, yearMovieCodes.size());
        }

        return new YearlyImportResult(
                normalizedStartYear,
                normalizedEndYear,
                normalizedTargetPerYear,
                allCollectedMovieCodes.size(),
                scannedWeekCount,
                savedRawCount,
                updatedRawCount,
                failedWeekCount,
                failedMovieInfoCount,
                collectedCountsByYear
        );
    }

    private void initializeRawTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS kobis_movie_raw (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    kobis_movie_cd VARCHAR(20) NOT NULL UNIQUE,
                    payload CLOB NOT NULL,
                    imported_at TIMESTAMP NOT NULL
                )
                """);
    }

    private boolean upsertRawMovie(String movieCd, String payload) {
        Integer existingCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM kobis_movie_raw
                WHERE kobis_movie_cd = ?
                """, Integer.class, movieCd);

        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
        if (existingCount != null && existingCount > 0) {
            jdbcTemplate.update("""
                    UPDATE kobis_movie_raw
                    SET payload = ?, imported_at = ?
                    WHERE kobis_movie_cd = ?
                    """, payload, now, movieCd);
            return false;
        }

        jdbcTemplate.update("""
                INSERT INTO kobis_movie_raw (kobis_movie_cd, payload, imported_at)
                VALUES (?, ?, ?)
                """, movieCd, payload, now);
        return true;
    }

    private URI buildWeeklyBoxOfficeUri(LocalDate targetDate) {
        return UriComponentsBuilder.fromUriString(baseUrl)
                .path("/boxoffice/searchWeeklyBoxOfficeList.json")
                .queryParam("key", apiKey)
                .queryParam("targetDt", targetDate.format(KOBIS_DATE))
                .queryParam("weekGb", "0")
                .build()
                .toUri();
    }

    private URI buildMovieInfoUri(String movieCd) {
        return UriComponentsBuilder.fromUriString(baseUrl)
                .path("/movie/searchMovieInfo.json")
                .queryParam("key", apiKey)
                .queryParam("movieCd", movieCd)
                .build()
                .toUri();
    }

    private String buildPayload(JsonNode weeklyEntry, JsonNode movieInfo) {
        return "{\"weeklyBoxOffice\":" + toJson(weeklyEntry) + ",\"movieInfo\":" + toJson(movieInfo) + "}";
    }

    private JsonNode readTree(String response) {
        try {
            return response == null ? null : objectMapper.readTree(response);
        } catch (JacksonException e) {
            throw new IllegalStateException("KOBIS response JSON parsing failed.", e);
        }
    }

    private String toJson(JsonNode jsonNode) {
        try {
            return objectMapper.writeValueAsString(jsonNode);
        } catch (JacksonException e) {
            throw new IllegalStateException("KOBIS JSON serialization failed.", e);
        }
    }

    private String textValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        if (value == null || value.isNull()) {
            return null;
        }
        String text = value.asText();
        return text.isBlank() ? null : text;
    }

    private void requireApiKey() {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("kobis.api-key is missing.");
        }
    }

    public record ImportResult(
            int collectedMovieCount,
            int scannedWeekCount,
            int savedRawCount,
            int updatedRawCount,
            int failedWeekCount,
            int failedMovieInfoCount
    ) {
    }

    public record YearlyImportResult(
            int startYear,
            int endYear,
            int targetPerYear,
            int collectedMovieCount,
            int scannedWeekCount,
            int savedRawCount,
            int updatedRawCount,
            int failedWeekCount,
            int failedMovieInfoCount,
            Map<Integer, Integer> collectedCountsByYear
    ) {
    }
}
