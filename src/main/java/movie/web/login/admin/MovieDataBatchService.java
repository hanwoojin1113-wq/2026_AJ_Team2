package movie.web.login.admin;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import movie.web.login.kobis.KobisMovieImportService;
import movie.web.login.kobis.KobisMovieNormalizeService;
import movie.web.login.recommendation.RecommendationBlockService;
import movie.web.login.recommendation.RecommendationMaintenanceService;
import movie.web.login.recommendation.RecommendationRankingService;
import movie.web.login.recommendation.RecommendationValidationService;
import movie.web.login.recommendation.UserPreferenceProfileService;
import movie.web.login.tag.MovieTagService;
import movie.web.login.tmdb.TmdbMovieImportService;
import movie.web.login.tmdb.TmdbMovieNormalizeService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
public class MovieDataBatchService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final KobisMovieImportService kobisMovieImportService;
    private final KobisMovieNormalizeService kobisMovieNormalizeService;
    private final TmdbMovieImportService tmdbMovieImportService;
    private final TmdbMovieNormalizeService tmdbMovieNormalizeService;
    private final MovieTagService movieTagService;
    private final UserPreferenceProfileService userPreferenceProfileService;
    private final RecommendationRankingService recommendationRankingService;
    private final RecommendationMaintenanceService recommendationMaintenanceService;
    private final RecommendationBlockService recommendationBlockService;
    private final RecommendationValidationService recommendationValidationService;
    private final DummyUserSeedService dummyUserSeedService;
    private final DummyUserActivitySeedService dummyUserActivitySeedService;

    public MovieDataBatchService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            KobisMovieImportService kobisMovieImportService,
            KobisMovieNormalizeService kobisMovieNormalizeService,
            TmdbMovieImportService tmdbMovieImportService,
            TmdbMovieNormalizeService tmdbMovieNormalizeService,
            MovieTagService movieTagService,
            UserPreferenceProfileService userPreferenceProfileService,
            RecommendationRankingService recommendationRankingService,
            RecommendationMaintenanceService recommendationMaintenanceService,
            RecommendationBlockService recommendationBlockService,
            RecommendationValidationService recommendationValidationService,
            DummyUserSeedService dummyUserSeedService,
            DummyUserActivitySeedService dummyUserActivitySeedService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.kobisMovieImportService = kobisMovieImportService;
        this.kobisMovieNormalizeService = kobisMovieNormalizeService;
        this.tmdbMovieImportService = tmdbMovieImportService;
        this.tmdbMovieNormalizeService = tmdbMovieNormalizeService;
        this.movieTagService = movieTagService;
        this.userPreferenceProfileService = userPreferenceProfileService;
        this.recommendationRankingService = recommendationRankingService;
        this.recommendationMaintenanceService = recommendationMaintenanceService;
        this.recommendationBlockService = recommendationBlockService;
        this.recommendationValidationService = recommendationValidationService;
        this.dummyUserSeedService = dummyUserSeedService;
        this.dummyUserActivitySeedService = dummyUserActivitySeedService;
    }

    public JobRunResponse runKobisYearlyImport(Integer startYear, Integer endYear, int targetPerYear) {
        return executeJob(
                "KOBIS_IMPORT_YEARLY",
                new KobisImportRequest(startYear, endYear, targetPerYear),
                () -> kobisMovieImportService.importYearlyRepresentativeMovies(startYear, endYear, targetPerYear)
        );
    }

    public JobRunResponse runKobisNormalize() {
        return executeJob("KOBIS_NORMALIZE", null, kobisMovieNormalizeService::normalizeRawMovies);
    }

    public JobRunResponse runTmdbPopularImport() {
        return executeJob("TMDB_IMPORT_POPULAR", null, tmdbMovieImportService::importPopularMovies);
    }

    public JobRunResponse runTmdbImportExisting(int limit) {
        return executeJob(
                "TMDB_IMPORT_EXISTING",
                new TmdbImportExistingRequest(limit),
                () -> tmdbMovieImportService.importExistingMovies(limit)
        );
    }

    public JobRunResponse runTmdbNormalize() {
        return executeJob("TMDB_NORMALIZE", null, tmdbMovieNormalizeService::normalizeRawMovies);
    }

    public JobRunResponse runTagRebuild() {
        return executeJob("TAG_REBUILD", null, movieTagService::rebuildTags);
    }

    public JobRunResponse runPreferenceProfileRebuild(Long userId) {
        return executeJob(
                "USER_PREFERENCE_PROFILE_REBUILD",
                new PreferenceProfileRebuildRequest(userId),
                () -> userPreferenceProfileService.rebuildProfile(userId)
        );
    }

    public JobRunResponse runRecommendationRankingRebuild(Long userId, Integer limit) {
        return executeJob(
                "USER_RECOMMENDATION_RANKING_REBUILD",
                new RecommendationRankingRebuildRequest(userId, limit),
                () -> recommendationRankingService.rebuildRanking(userId, limit)
        );
    }

    public JobRunResponse runTestUserSeed(boolean reset) {
        return executeJob(
                "TEST_USER_SEED",
                new TestUserSeedRequest(reset),
                () -> dummyUserSeedService.seedTestUsers(reset)
        );
    }

    public JobRunResponse runTestUserActivitySeed(boolean reset, boolean refreshRecommendations) {
        return executeJob(
                "TEST_USER_ACTIVITY_SEED",
                new TestUserActivitySeedRequest(reset, refreshRecommendations),
                () -> dummyUserActivitySeedService.seedActivities(reset, refreshRecommendations)
        );
    }

    public JobRunResponse runRefreshDirtyRecommendations(int batchSize, Integer limit) {
        return executeJob(
                "USER_RECOMMENDATION_REFRESH_DIRTY",
                new RefreshDirtyRecommendationsRequest(batchSize, limit),
                () -> recommendationMaintenanceService.refreshDirtyUsers(batchSize, limit)
        );
    }

    public RecommendationBlockService.RecommendationBlockResponse previewRecommendationBlocks(
            Long userId,
            boolean refreshIfDirty,
            Integer sliceLimit,
            Integer itemLimit
    ) {
        if (refreshIfDirty) {
            recommendationMaintenanceService.ensureRecommendations(userId, null);
        }
        return recommendationBlockService.buildBlocks(userId, sliceLimit, itemLimit);
    }

    public RecommendationValidationService.ValidationResponse validateRecommendations(
            String loginId,
            boolean refreshIfDirty,
            Integer topRecommendationLimit
    ) {
        return recommendationValidationService.inspectTestUsers(loginId, refreshIfDirty, topRecommendationLimit);
    }

    public PipelineRunResponse runPipeline(Integer startYear, Integer endYear, int targetPerYear, int tmdbLimit) {
        JobRunResponse kobisImport = runKobisYearlyImport(startYear, endYear, targetPerYear);
        if (!kobisImport.isSuccess()) {
            return new PipelineRunResponse(kobisImport, null, null, null, null);
        }
        JobRunResponse kobisNormalize = runKobisNormalize();
        if (!kobisNormalize.isSuccess()) {
            return new PipelineRunResponse(kobisImport, kobisNormalize, null, null, null);
        }
        JobRunResponse tmdbImport = runTmdbImportExisting(tmdbLimit);
        if (!tmdbImport.isSuccess()) {
            return new PipelineRunResponse(kobisImport, kobisNormalize, tmdbImport, null, null);
        }
        JobRunResponse tmdbNormalize = runTmdbNormalize();
        if (!tmdbNormalize.isSuccess()) {
            return new PipelineRunResponse(kobisImport, kobisNormalize, tmdbImport, tmdbNormalize, null);
        }
        JobRunResponse tagRebuild = runTagRebuild();
        return new PipelineRunResponse(kobisImport, kobisNormalize, tmdbImport, tmdbNormalize, tagRebuild);
    }

    public List<JobRunHistory> listRuns(int limit) {
        initializeBatchRunTable();
        int normalizedLimit = Math.max(1, Math.min(limit, 100));
        return jdbcTemplate.query("""
                SELECT id, job_name, status, request_payload, result_payload, error_message, started_at, finished_at
                FROM batch_job_run
                ORDER BY id DESC
                LIMIT ?
                """, (rs, rowNum) -> new JobRunHistory(
                rs.getLong("id"),
                rs.getString("job_name"),
                rs.getString("status"),
                rs.getString("request_payload"),
                rs.getString("result_payload"),
                rs.getString("error_message"),
                rs.getTimestamp("started_at").toLocalDateTime(),
                rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toLocalDateTime()
        ), normalizedLimit);
    }

    private JobRunResponse executeJob(String jobName, Object request, CheckedSupplier<Object> action) {
        initializeBatchRunTable();

        LocalDateTime startedAt = LocalDateTime.now();
        Long runId = insertRunningJob(jobName, serialize(request), startedAt);

        try {
            Object result = action.get();
            LocalDateTime finishedAt = LocalDateTime.now();
            String resultPayload = serialize(result);
            jdbcTemplate.update("""
                    UPDATE batch_job_run
                    SET status = ?, result_payload = ?, finished_at = ?
                    WHERE id = ?
                    """, "SUCCESS", resultPayload, Timestamp.valueOf(finishedAt), runId);
            return new JobRunResponse(runId, jobName, "SUCCESS", startedAt, finishedAt, result, null);
        } catch (RuntimeException e) {
            LocalDateTime finishedAt = LocalDateTime.now();
            String errorMessage = trimErrorMessage(e);
            jdbcTemplate.update("""
                    UPDATE batch_job_run
                    SET status = ?, error_message = ?, finished_at = ?
                    WHERE id = ?
                    """, "FAILED", errorMessage, Timestamp.valueOf(finishedAt), runId);
            return new JobRunResponse(runId, jobName, "FAILED", startedAt, finishedAt, null, errorMessage);
        }
    }

    private void initializeBatchRunTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS batch_job_run (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    job_name VARCHAR(100) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    request_payload CLOB,
                    result_payload CLOB,
                    error_message CLOB,
                    started_at TIMESTAMP NOT NULL,
                    finished_at TIMESTAMP
                )
                """);
    }

    private Long insertRunningJob(String jobName, String requestPayload, LocalDateTime startedAt) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO batch_job_run (job_name, status, request_payload, started_at)
                    VALUES (?, ?, ?, ?)
                    """, new String[]{"id"});
            statement.setString(1, jobName);
            statement.setString(2, "RUNNING");
            statement.setString(3, requestPayload);
            statement.setTimestamp(4, Timestamp.valueOf(startedAt));
            return statement;
        }, keyHolder);
        return keyHolder.getKeyAs(Long.class);
    }

    private String serialize(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JacksonException e) {
            return String.valueOf(value);
        }
    }

    private String trimErrorMessage(Throwable throwable) {
        String message = throwable.getMessage();
        if (message == null || message.isBlank()) {
            return throwable.getClass().getName();
        }
        return message.length() > 4000 ? message.substring(0, 4000) : message;
    }

    @FunctionalInterface
    private interface CheckedSupplier<T> {
        T get();
    }

    public record JobRunResponse(
            Long runId,
            String jobName,
            String status,
            LocalDateTime startedAt,
            LocalDateTime finishedAt,
            Object result,
            String errorMessage
    ) {
        public boolean isSuccess() {
            return "SUCCESS".equals(status);
        }
    }

    public record PipelineRunResponse(
            JobRunResponse kobisImport,
            JobRunResponse kobisNormalize,
            JobRunResponse tmdbImport,
            JobRunResponse tmdbNormalize,
            JobRunResponse tagRebuild
    ) {
    }

    public record JobRunHistory(
            Long id,
            String jobName,
            String status,
            String requestPayload,
            String resultPayload,
            String errorMessage,
            LocalDateTime startedAt,
            LocalDateTime finishedAt
    ) {
    }

    private record KobisImportRequest(Integer startYear, Integer endYear, int targetPerYear) {
    }

    private record TmdbImportExistingRequest(int limit) {
    }

    private record PreferenceProfileRebuildRequest(Long userId) {
    }

    private record RecommendationRankingRebuildRequest(Long userId, Integer limit) {
    }

    private record TestUserSeedRequest(boolean reset) {
    }

    private record TestUserActivitySeedRequest(boolean reset, boolean refreshRecommendations) {
    }

    private record RefreshDirtyRecommendationsRequest(int batchSize, Integer limit) {
    }
}
