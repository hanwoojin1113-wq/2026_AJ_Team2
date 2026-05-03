package com.cinematch.recommendation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class RecommendationBlockService {

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationFeaturePolicy recommendationFeaturePolicy;
    private final RecommendationMovieFilterService recommendationMovieFilterService;

    public RecommendationBlockService(
            JdbcTemplate jdbcTemplate,
            RecommendationFeaturePolicy recommendationFeaturePolicy,
            RecommendationMovieFilterService recommendationMovieFilterService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationFeaturePolicy = recommendationFeaturePolicy;
        this.recommendationMovieFilterService = recommendationMovieFilterService;
    }

    public RecommendationBlockResponse buildBlocks(Long userId) {
        return buildBlocks(userId, recommendationFeaturePolicy.blockSliceLimit(), recommendationFeaturePolicy.blockItemLimit());
    }

    public RecommendationBlockResponse buildBlocks(Long userId, Integer sliceLimit, Integer itemLimit) {
        int normalizedSliceLimit = Math.max(
                20,
                Math.min(sliceLimit == null ? recommendationFeaturePolicy.blockSliceLimit() : sliceLimit, 160)
        );
        int normalizedItemLimit = Math.max(
                4,
                Math.min(itemLimit == null ? recommendationFeaturePolicy.blockItemLimit() : itemLimit, 12)
        );

        List<RankedMovie> rankingSlice = loadRankingSlice(userId, normalizedSliceLimit);
        if (rankingSlice.isEmpty()) {
            return new RecommendationBlockResponse(userId, normalizedSliceLimit, normalizedItemLimit, 0, List.of());
        }

        Set<Long> movieIds = rankingSlice.stream()
                .map(RankedMovie::movieId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<Long> recommendableMovieIds = recommendationMovieFilterService.filterRecommendableMovieIds(movieIds);
        rankingSlice = rankingSlice.stream()
                .filter(movie -> recommendableMovieIds.contains(movie.movieId()))
                .toList();
        if (rankingSlice.isEmpty()) {
            return new RecommendationBlockResponse(userId, normalizedSliceLimit, normalizedItemLimit, 0, List.of());
        }

        movieIds = rankingSlice.stream()
                .map(RankedMovie::movieId)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Long> actorEligibleMovieIds = recommendationMovieFilterService.filterActorEligibleMovieIds(movieIds);

        Map<Long, Set<String>> tagsByMovie = loadNamedFeatureSetMap(movieIds, """
                SELECT mt.movie_id, t.tag_name AS feature_name
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                WHERE mt.movie_id IN (%s)
                  AND t.tag_type <> 'CAUTION'
                """);
        Map<Long, Set<String>> genresByMovie = loadNamedFeatureSetMap(movieIds, """
                SELECT mg.movie_id, g.name AS feature_name
                FROM movie_genre mg
                JOIN genre g ON g.id = mg.genre_id
                WHERE mg.movie_id IN (%s)
                """);
        Map<Long, Set<String>> directorsByMovie = loadNamedFeatureSetMap(movieIds, """
                SELECT md.movie_id, p.name AS feature_name
                FROM movie_director md
                JOIN person p ON p.id = md.person_id
                WHERE md.movie_id IN (%s)
                """);
        Map<Long, Set<String>> actorsByMovie = actorEligibleMovieIds.isEmpty()
                ? Collections.emptyMap()
                : loadNamedFeatureSetMap(actorEligibleMovieIds, """
                SELECT ma.movie_id, p.name AS feature_name
                FROM movie_actor ma
                JOIN person p ON p.id = ma.person_id
                WHERE ma.movie_id IN (%s)
                  AND ma.display_order <= %d
                """.formatted("%s", recommendationFeaturePolicy.actorLimit()));
        Map<Long, Set<String>> providersByMovie = loadNamedFeatureSetMap(movieIds, """
                SELECT DISTINCT mp.movie_id, p.provider_name AS feature_name
                FROM movie_provider mp
                JOIN provider p ON p.id = mp.provider_id
                WHERE mp.movie_id IN (%s)
                  AND mp.provider_type = '%s'
                  AND mp.region_code = '%s'
                """.formatted("%s",
                recommendationFeaturePolicy.preferredProviderType(),
                recommendationFeaturePolicy.preferredProviderRegionCode()));

        Map<String, Double> tagProfileScores = loadProfileScores(userId, "TAG");
        Map<String, Double> genreProfileScores = loadProfileScores(userId, "GENRE");
        Map<String, Double> directorProfileScores = loadProfileScores(userId, "DIRECTOR");
        Map<String, Double> actorProfileScores = loadProfileScores(userId, "ACTOR");
        Map<String, Double> providerProfileScores = loadProfileScores(userId, "PROVIDER");

        PeopleEvidenceSnapshot peopleEvidence = loadPeopleEvidence(userId);

        List<InternalBlock> thematicBlocks = new ArrayList<>();
        thematicBlocks.addAll(collectFeatureBlocks(
                "TAG",
                rankingSlice,
                tagsByMovie,
                tagProfileScores,
                normalizedItemLimit,
                2,
                Map.of()
        ));
        thematicBlocks.addAll(collectFeatureBlocks(
                "GENRE",
                rankingSlice,
                genresByMovie,
                genreProfileScores,
                normalizedItemLimit,
                1,
                Map.of()
        ));
        thematicBlocks.addAll(collectFeatureBlocks(
                "DIRECTOR",
                rankingSlice,
                directorsByMovie,
                directorProfileScores,
                normalizedItemLimit,
                1,
                peopleEvidence.directorEvidence()
        ));
        thematicBlocks.addAll(collectFeatureBlocks(
                "ACTOR",
                rankingSlice,
                actorsByMovie,
                actorProfileScores,
                normalizedItemLimit,
                1,
                peopleEvidence.actorEvidence()
        ));
        thematicBlocks.addAll(collectFeatureBlocks(
                "PROVIDER",
                rankingSlice,
                providersByMovie,
                providerProfileScores,
                normalizedItemLimit,
                1,
                Map.of()
        ));

        Map<Long, Set<String>> thematicMembershipByMovieId = buildThematicMembershipMap(thematicBlocks);
        RecommendationBlock personalizedBlock = buildPersonalizedBlock(
                rankingSlice,
                tagsByMovie,
                genresByMovie,
                directorsByMovie,
                tagProfileScores,
                genreProfileScores,
                directorProfileScores,
                thematicMembershipByMovieId,
                normalizedItemLimit
        );

        List<RecommendationBlock> blocks = new ArrayList<>();
        blocks.add(personalizedBlock);
        thematicBlocks.stream()
                .map(InternalBlock::toExternalBlock)
                .forEach(blocks::add);

        return new RecommendationBlockResponse(userId, normalizedSliceLimit, normalizedItemLimit, blocks.size(), blocks);
    }

    private List<InternalBlock> collectFeatureBlocks(
            String featureType,
            List<RankedMovie> rankingSlice,
            Map<Long, Set<String>> featuresByMovie,
            Map<String, Double> profileScores,
            int itemLimit,
            int maxBlocks,
            Map<String, PersonFeatureEvidence> personEvidenceByName
    ) {
        List<FeatureAggregate> candidates = collectFeatureAggregates(rankingSlice, featuresByMovie, profileScores).stream()
                .filter(candidate -> isBlockCandidateAllowed(featureType, candidate, personEvidenceByName))
                .sorted(Comparator
                        .comparingDouble(FeatureAggregate::profileScore).reversed()
                        .thenComparing(Comparator.comparingInt(FeatureAggregate::sampleCount).reversed())
                        .thenComparingDouble(FeatureAggregate::averageRank))
                .limit(maxBlocks)
                .toList();

        List<InternalBlock> blocks = new ArrayList<>();
        for (FeatureAggregate candidate : candidates) {
            List<RankedMovie> items = rankingSlice.stream()
                    .filter(movie -> featuresByMovie.getOrDefault(movie.movieId(), Collections.emptySet()).contains(candidate.featureName()))
                    .limit(itemLimit)
                    .toList();
            if (items.size() < recommendationFeaturePolicy.blockMinimumSize()) {
                continue;
            }

            blocks.add(new InternalBlock(
                    featureType + ":" + candidate.featureName(),
                    blockTitle(featureType, candidate.featureName()),
                    blockDescription(featureType, candidate.featureName()),
                    items
            ));
        }
        return blocks;
    }

    private RecommendationBlock buildPersonalizedBlock(
            List<RankedMovie> rankingSlice,
            Map<Long, Set<String>> tagsByMovie,
            Map<Long, Set<String>> genresByMovie,
            Map<Long, Set<String>> directorsByMovie,
            Map<String, Double> tagProfileScores,
            Map<String, Double> genreProfileScores,
            Map<String, Double> directorProfileScores,
            Map<Long, Set<String>> thematicMembershipByMovieId,
            int itemLimit
    ) {
        int poolSize = Math.min(recommendationFeaturePolicy.personalizedPoolSize(), rankingSlice.size());
        List<PersonalizedCandidate> pool = rankingSlice.stream()
                .limit(poolSize)
                .map(movie -> toPersonalizedCandidate(
                        movie,
                        tagsByMovie,
                        genresByMovie,
                        directorsByMovie,
                        tagProfileScores,
                        genreProfileScores,
                        directorProfileScores,
                        thematicMembershipByMovieId
                ))
                .toList();

        List<PersonalizedCandidate> selected = selectPersonalizedCandidates(pool, itemLimit);
        List<BlockMovie> items = selected.stream()
                .map(candidate -> candidate.movie().toBlockMovie())
                .toList();

        return new RecommendationBlock(
                "PERSONALIZED",
                "회원님을 위한 추천",
                "개인화 랭킹 상위 후보를 다양성 있게 다시 구성한 대표 추천",
                items
        );
    }

    private List<PersonalizedCandidate> selectPersonalizedCandidates(
            List<PersonalizedCandidate> pool,
            int itemLimit
    ) {
        List<PersonalizedCandidate> selected = new ArrayList<>();
        Set<Long> selectedMovieIds = new LinkedHashSet<>();
        Map<String, Integer> genreCounts = new LinkedHashMap<>();
        Map<String, Integer> tagCounts = new LinkedHashMap<>();
        Map<String, Integer> directorCounts = new LinkedHashMap<>();
        Map<String, Integer> reasonCounts = new LinkedHashMap<>();
        Map<String, Integer> overlapCountsByBlockKey = new LinkedHashMap<>();

        while (selected.size() < itemLimit) {
            PersonalizedCandidate bestCandidate = null;
            double bestScore = Double.NEGATIVE_INFINITY;

            for (PersonalizedCandidate candidate : pool) {
                if (selectedMovieIds.contains(candidate.movie().movieId())) {
                    continue;
                }
                if (!isPersonalizedCandidateAllowed(
                        candidate,
                        genreCounts,
                        tagCounts,
                        directorCounts,
                        reasonCounts,
                        overlapCountsByBlockKey
                )) {
                    continue;
                }

                double selectionScore = computePersonalizedSelectionScore(
                        candidate,
                        genreCounts,
                        tagCounts,
                        directorCounts,
                        reasonCounts,
                        overlapCountsByBlockKey
                );
                if (bestCandidate == null
                        || selectionScore > bestScore
                        || (selectionScore == bestScore && candidate.movie().rankNo() < bestCandidate.movie().rankNo())) {
                    bestCandidate = candidate;
                    bestScore = selectionScore;
                }
            }

            if (bestCandidate == null) {
                break;
            }

            selected.add(bestCandidate);
            selectedMovieIds.add(bestCandidate.movie().movieId());
            incrementCount(genreCounts, bestCandidate.dominantGenre());
            incrementCount(tagCounts, bestCandidate.dominantTag());
            incrementCount(directorCounts, bestCandidate.dominantDirector());
            incrementCount(reasonCounts, bestCandidate.reasonPattern());
            for (String blockKey : bestCandidate.thematicBlockKeys()) {
                incrementCount(overlapCountsByBlockKey, blockKey);
            }
        }

        return selected;
    }

    private boolean isPersonalizedCandidateAllowed(
            PersonalizedCandidate candidate,
            Map<String, Integer> genreCounts,
            Map<String, Integer> tagCounts,
            Map<String, Integer> directorCounts,
            Map<String, Integer> reasonCounts,
            Map<String, Integer> overlapCountsByBlockKey
    ) {
        if (countOf(genreCounts, candidate.dominantGenre()) >= recommendationFeaturePolicy.personalizedGenreCap()) {
            return false;
        }
        if (countOf(tagCounts, candidate.dominantTag()) >= recommendationFeaturePolicy.personalizedTagCap()) {
            return false;
        }
        if (countOf(directorCounts, candidate.dominantDirector()) >= recommendationFeaturePolicy.personalizedDirectorCap()) {
            return false;
        }
        if (countOf(reasonCounts, candidate.reasonPattern()) >= recommendationFeaturePolicy.personalizedReasonCap()) {
            return false;
        }
        for (String blockKey : candidate.thematicBlockKeys()) {
            if (countOf(overlapCountsByBlockKey, blockKey) >= recommendationFeaturePolicy.thematicOverlapCap()) {
                return false;
            }
        }
        return true;
    }

    private double computePersonalizedSelectionScore(
            PersonalizedCandidate candidate,
            Map<String, Integer> genreCounts,
            Map<String, Integer> tagCounts,
            Map<String, Integer> directorCounts,
            Map<String, Integer> reasonCounts,
            Map<String, Integer> overlapCountsByBlockKey
    ) {
        double score = candidate.movie().finalScore();
        score -= repeatPenalty(countOf(genreCounts, candidate.dominantGenre()), recommendationFeaturePolicy.genreRepeatPenaltyWeight());
        score -= repeatPenalty(countOf(tagCounts, candidate.dominantTag()), recommendationFeaturePolicy.tagRepeatPenaltyWeight());
        score -= repeatPenalty(countOf(directorCounts, candidate.dominantDirector()), recommendationFeaturePolicy.directorRepeatPenaltyWeight());
        score -= repeatPenalty(countOf(reasonCounts, candidate.reasonPattern()), recommendationFeaturePolicy.reasonRepeatPenaltyWeight());

        double thematicOverlapPenalty = candidate.thematicBlockKeys().stream()
                .mapToDouble(blockKey -> repeatPenalty(
                        countOf(overlapCountsByBlockKey, blockKey),
                        recommendationFeaturePolicy.thematicOverlapPenaltyWeight()
                ))
                .sum();
        score -= thematicOverlapPenalty;

        score += diversityBonus(candidate, genreCounts, tagCounts, directorCounts, reasonCounts);
        return score;
    }

    private double diversityBonus(
            PersonalizedCandidate candidate,
            Map<String, Integer> genreCounts,
            Map<String, Integer> tagCounts,
            Map<String, Integer> directorCounts,
            Map<String, Integer> reasonCounts
    ) {
        double bonus = 0.0;
        if (candidate.dominantGenre() != null && !genreCounts.containsKey(candidate.dominantGenre())) {
            bonus += recommendationFeaturePolicy.mildDiversityBonusWeight();
        }
        if (candidate.dominantTag() != null && !tagCounts.containsKey(candidate.dominantTag())) {
            bonus += recommendationFeaturePolicy.mildDiversityBonusWeight();
        }
        if (candidate.dominantDirector() != null && !directorCounts.containsKey(candidate.dominantDirector())) {
            bonus += recommendationFeaturePolicy.mildDiversityBonusWeight();
        }
        if (candidate.reasonPattern() != null && !reasonCounts.containsKey(candidate.reasonPattern())) {
            bonus += recommendationFeaturePolicy.mildDiversityBonusWeight();
        }
        return bonus;
    }

    private double repeatPenalty(int currentCount, double weight) {
        if (currentCount <= 0) {
            return 0.0;
        }
        return currentCount * currentCount * weight;
    }

    private PersonalizedCandidate toPersonalizedCandidate(
            RankedMovie movie,
            Map<Long, Set<String>> tagsByMovie,
            Map<Long, Set<String>> genresByMovie,
            Map<Long, Set<String>> directorsByMovie,
            Map<String, Double> tagProfileScores,
            Map<String, Double> genreProfileScores,
            Map<String, Double> directorProfileScores,
            Map<Long, Set<String>> thematicMembershipByMovieId
    ) {
        return new PersonalizedCandidate(
                movie,
                dominantFeatureName(genresByMovie.getOrDefault(movie.movieId(), Collections.emptySet()), genreProfileScores),
                dominantFeatureName(tagsByMovie.getOrDefault(movie.movieId(), Collections.emptySet()), tagProfileScores),
                dominantFeatureName(directorsByMovie.getOrDefault(movie.movieId(), Collections.emptySet()), directorProfileScores),
                normalizeReasonPattern(movie.reasonSummary()),
                thematicMembershipByMovieId.getOrDefault(movie.movieId(), Collections.emptySet())
        );
    }

    private String dominantFeatureName(Set<String> featureNames, Map<String, Double> profileScores) {
        return featureNames.stream()
                .sorted(Comparator
                        .comparingDouble((String featureName) -> profileScores.getOrDefault(featureName, 0.0)).reversed()
                        .thenComparing(Comparator.naturalOrder()))
                .findFirst()
                .orElse(null);
    }

    private String normalizeReasonPattern(String reasonSummary) {
        if (reasonSummary == null || reasonSummary.isBlank()) {
            return null;
        }

        String normalized = reasonSummary.toLowerCase(Locale.ROOT);
        List<String> parts = new ArrayList<>();
        if (normalized.contains("태그")) {
            parts.add("TAG");
        }
        if (normalized.contains("장르")) {
            parts.add("GENRE");
        }
        if (normalized.contains("감독")) {
            parts.add("DIRECTOR");
        }
        if (normalized.contains("배우")) {
            parts.add("ACTOR");
        }
        if (normalized.contains("키워드")) {
            parts.add("KEYWORD");
        }
        if (normalized.contains("시청")) {
            parts.add("PROVIDER");
        }
        if (parts.isEmpty()) {
            return normalized.trim();
        }
        return String.join("+", parts);
    }

    private Map<Long, Set<String>> buildThematicMembershipMap(List<InternalBlock> thematicBlocks) {
        Map<Long, Set<String>> membershipByMovieId = new LinkedHashMap<>();
        for (InternalBlock block : thematicBlocks) {
            for (RankedMovie movie : block.items()) {
                membershipByMovieId
                        .computeIfAbsent(movie.movieId(), ignored -> new LinkedHashSet<>())
                        .add(block.key());
            }
        }
        return membershipByMovieId;
    }

    private boolean isBlockCandidateAllowed(
            String featureType,
            FeatureAggregate candidate,
            Map<String, PersonFeatureEvidence> personEvidenceByName
    ) {
        if (candidate.sampleCount() < recommendationFeaturePolicy.blockMinimumSize()) {
            return false;
        }

        if ("GENRE".equals(featureType)) {
            return recommendationFeaturePolicy.isGenreBlockAllowed(candidate.featureName(), candidate.sampleCount());
        }

        if ("DIRECTOR".equals(featureType)) {
            PersonFeatureEvidence evidence = personEvidenceByName.get(candidate.featureName());
            return evidence != null
                    && evidence.sourceMovieCount() >= recommendationFeaturePolicy.minimumDirectorBlockSourceMovies()
                    && evidence.signalWeightSum() >= recommendationFeaturePolicy.minimumDirectorBlockSignalWeight();
        }

        if ("ACTOR".equals(featureType)) {
            PersonFeatureEvidence evidence = personEvidenceByName.get(candidate.featureName());
            return evidence != null
                    && evidence.sourceMovieCount() >= recommendationFeaturePolicy.minimumActorBlockSourceMovies()
                    && evidence.signalWeightSum() >= recommendationFeaturePolicy.minimumActorBlockSignalWeight();
        }

        return true;
    }

    private List<FeatureAggregate> collectFeatureAggregates(
            List<RankedMovie> rankingSlice,
            Map<Long, Set<String>> featuresByMovie,
            Map<String, Double> profileScores
    ) {
        Map<String, MutableAggregate> aggregates = new LinkedHashMap<>();

        for (RankedMovie movie : rankingSlice) {
            for (String featureName : featuresByMovie.getOrDefault(movie.movieId(), Collections.emptySet())) {
                MutableAggregate aggregate = aggregates.computeIfAbsent(featureName, ignored -> new MutableAggregate(featureName));
                aggregate.sampleCount++;
                aggregate.totalRank += movie.rankNo();
                aggregate.profileScore = Math.max(aggregate.profileScore, profileScores.getOrDefault(featureName, 0.0));
            }
        }

        return aggregates.values().stream()
                .map(value -> new FeatureAggregate(
                        value.featureName,
                        value.sampleCount,
                        value.totalRank / (double) value.sampleCount,
                        value.profileScore
                ))
                .toList();
    }

    private PeopleEvidenceSnapshot loadPeopleEvidence(Long userId) {
        Map<Long, Double> signalWeightsByMovie = loadSignalWeights(userId);
        if (signalWeightsByMovie.isEmpty()) {
            return new PeopleEvidenceSnapshot(Map.of(), Map.of());
        }

        signalWeightsByMovie.keySet().retainAll(
                recommendationMovieFilterService.filterRecommendableMovieIds(signalWeightsByMovie.keySet())
        );
        if (signalWeightsByMovie.isEmpty()) {
            return new PeopleEvidenceSnapshot(Map.of(), Map.of());
        }

        Map<Long, Set<String>> directorsByMovie = loadNamedFeatureSetMap(signalWeightsByMovie.keySet(), """
                SELECT md.movie_id, p.name AS feature_name
                FROM movie_director md
                JOIN person p ON p.id = md.person_id
                WHERE md.movie_id IN (%s)
                """);

        Set<Long> actorEligibleMovieIds =
                recommendationMovieFilterService.filterActorEligibleMovieIds(signalWeightsByMovie.keySet());
        Map<Long, Set<String>> actorsByMovie = actorEligibleMovieIds.isEmpty()
                ? Collections.emptyMap()
                : loadNamedFeatureSetMap(actorEligibleMovieIds, """
                SELECT ma.movie_id, p.name AS feature_name
                FROM movie_actor ma
                JOIN person p ON p.id = ma.person_id
                WHERE ma.movie_id IN (%s)
                  AND ma.display_order <= %d
                """.formatted("%s", recommendationFeaturePolicy.actorLimit()));

        return new PeopleEvidenceSnapshot(
                collectPersonEvidence(signalWeightsByMovie, directorsByMovie),
                collectPersonEvidence(signalWeightsByMovie, actorsByMovie)
        );
    }

    private Map<String, PersonFeatureEvidence> collectPersonEvidence(
            Map<Long, Double> signalWeightsByMovie,
            Map<Long, Set<String>> featuresByMovie
    ) {
        Map<String, MutablePersonEvidence> evidenceByName = new LinkedHashMap<>();

        for (Map.Entry<Long, Double> entry : signalWeightsByMovie.entrySet()) {
            Set<String> featureNames = featuresByMovie.getOrDefault(entry.getKey(), Collections.emptySet());
            if (featureNames.isEmpty()) {
                continue;
            }
            for (String featureName : featureNames) {
                MutablePersonEvidence evidence =
                        evidenceByName.computeIfAbsent(featureName, ignored -> new MutablePersonEvidence());
                evidence.sourceMovieCount++;
                evidence.signalWeightSum += entry.getValue();
            }
        }

        return evidenceByName.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new PersonFeatureEvidence(
                                entry.getValue().sourceMovieCount,
                                roundScore(entry.getValue().signalWeightSum)
                        ),
                        (left, right) -> left,
                        LinkedHashMap::new
                ));
    }

    private Map<Long, Double> loadSignalWeights(Long userId) {
        initializeWatchedSignalTable();
        Map<Long, Double> signalWeightsByMovie = new LinkedHashMap<>();

        mergeFixedSignal(signalWeightsByMovie, userId, """
                SELECT movie_id
                FROM user_movie_life
                WHERE user_id = ?
                """, recommendationFeaturePolicy.lifeSignalWeight());

        mergeFixedSignal(signalWeightsByMovie, userId, """
                SELECT movie_id
                FROM user_movie_like
                WHERE user_id = ?
                  AND liked = TRUE
                """, recommendationFeaturePolicy.likeSignalWeight());

        jdbcTemplate.query("""
                SELECT movie_id, rating
                FROM user_movie_watched
                WHERE user_id = ?
                  AND COALESCE(status, 'WATCHED') = 'WATCHED'
                """, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            signalWeightsByMovie.merge(
                    rs.getLong("movie_id"),
                    recommendationFeaturePolicy.watchedSignalWeight(rs.getObject("rating", Integer.class)),
                    Double::sum
            );
        }, userId);

        mergeFixedSignal(signalWeightsByMovie, userId, """
                SELECT movie_id
                FROM user_movie_store
                WHERE user_id = ?
                """, recommendationFeaturePolicy.storeSignalWeight());

        return signalWeightsByMovie;
    }

    private void mergeFixedSignal(Map<Long, Double> signalWeightsByMovie, Long userId, String sql, double weight) {
        jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            signalWeightsByMovie.merge(
                    rs.getLong("movie_id"),
                    weight,
                    Double::sum
            );
        }, userId);
    }

    private void initializeWatchedSignalTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_movie_watched (
                    user_id BIGINT NOT NULL,
                    movie_id BIGINT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'WATCHED',
                    rating INT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, movie_id),
                    CONSTRAINT fk_user_movie_watched_user FOREIGN KEY (user_id) REFERENCES "USER"(id),
                    CONSTRAINT fk_user_movie_watched_movie FOREIGN KEY (movie_id) REFERENCES movie(id)
                )
                """);
        jdbcTemplate.execute("""
                ALTER TABLE user_movie_watched
                ADD COLUMN IF NOT EXISTS rating INT
                """);
        jdbcTemplate.execute("""
                ALTER TABLE user_movie_watched
                ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'WATCHED'
                """);
        jdbcTemplate.update("""
                UPDATE user_movie_watched
                SET status = 'WATCHED'
                WHERE status IS NULL
                """);
    }

    private List<RankedMovie> loadRankingSlice(Long userId, int sliceLimit) {
        return jdbcTemplate.query("""
                SELECT
                    urr.movie_id,
                    urr.rank_no,
                    urr.final_score,
                    COALESCE(m.title, m.movie_name) AS display_title,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original, m.movie_cd) AS display_subtitle,
                    m.movie_cd,
                    m.poster_image_url,
                    urr.reason_summary
                FROM user_recommendation_result urr
                JOIN movie m ON m.id = urr.movie_id
                WHERE urr.user_id = ?
                  AND m.poster_image_url IS NOT NULL
                  AND m.poster_image_url <> ''
                ORDER BY urr.rank_no
                LIMIT ?
                """, (rs, rowNum) -> new RankedMovie(
                rs.getLong("movie_id"),
                rs.getInt("rank_no"),
                rs.getDouble("final_score"),
                rs.getString("display_title"),
                rs.getString("display_subtitle"),
                rs.getString("movie_cd"),
                rs.getString("poster_image_url"),
                rs.getString("reason_summary")
        ), userId, sliceLimit);
    }

    private Map<String, Double> loadProfileScores(Long userId, String featureType) {
        Map<String, Double> scores = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT feature_name, score
                FROM user_preference_profile
                WHERE user_id = ?
                  AND feature_type = ?
                ORDER BY score DESC, feature_name ASC
                """, (org.springframework.jdbc.core.RowCallbackHandler) rs ->
                scores.put(rs.getString("feature_name"), rs.getDouble("score")), userId, featureType);
        return scores;
    }

    private Map<Long, Set<String>> loadNamedFeatureSetMap(Set<Long> movieIds, String template) {
        if (movieIds.isEmpty()) {
            return Collections.emptyMap();
        }

        String sql = template.formatted(placeholders(movieIds.size()));
        Map<Long, Set<String>> featuresByMovie = new LinkedHashMap<>();
        List<Object> params = new ArrayList<>(movieIds);

        jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            long movieId = rs.getLong("movie_id");
            String featureName = rs.getString("feature_name");
            if (featureName == null || featureName.isBlank()) {
                return;
            }
            featuresByMovie.computeIfAbsent(movieId, ignored -> new LinkedHashSet<>()).add(featureName);
        }, params.toArray());

        return featuresByMovie;
    }

    private String blockTitle(String featureType, String featureName) {
        return switch (featureType) {
            case "TAG" -> recommendationFeaturePolicy.tagBlockTitle(featureName);
            case "GENRE" -> featureName + " 영화 추천";
            case "DIRECTOR" -> featureName + " 감독 작품 추천";
            case "ACTOR" -> featureName + " 출연작 추천";
            case "PROVIDER" -> featureName + "에서 볼 수 있는 추천";
            default -> featureName + " 추천";
        };
    }

    private String blockDescription(String featureType, String featureName) {
        return switch (featureType) {
            case "TAG" -> "개인화 랭킹 상위 결과에서 추출한 태그 블록";
            case "GENRE" -> "개인화 랭킹 기반 장르 블록";
            case "DIRECTOR" -> "반복적으로 반응한 감독과 연결된 작품";
            case "ACTOR" -> "충분한 근거가 있는 배우 연결 작품";
            case "PROVIDER" -> "선호 OTT 제공 작품";
            default -> featureName;
        };
    }

    private String placeholders(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "?")
                .collect(Collectors.joining(", "));
    }

    private double roundScore(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    private int countOf(Map<String, Integer> counts, String key) {
        if (key == null || key.isBlank()) {
            return 0;
        }
        return counts.getOrDefault(key, 0);
    }

    private void incrementCount(Map<String, Integer> counts, String key) {
        if (key == null || key.isBlank()) {
            return;
        }
        counts.merge(key, 1, Integer::sum);
    }

    private static final class MutableAggregate {
        private final String featureName;
        private int sampleCount;
        private int totalRank;
        private double profileScore;

        private MutableAggregate(String featureName) {
            this.featureName = featureName;
        }
    }

    private static final class MutablePersonEvidence {
        private int sourceMovieCount;
        private double signalWeightSum;
    }

    private record FeatureAggregate(
            String featureName,
            int sampleCount,
            double averageRank,
            double profileScore
    ) {
    }

    private record PersonFeatureEvidence(
            int sourceMovieCount,
            double signalWeightSum
    ) {
    }

    private record PeopleEvidenceSnapshot(
            Map<String, PersonFeatureEvidence> directorEvidence,
            Map<String, PersonFeatureEvidence> actorEvidence
    ) {
    }

    private record RankedMovie(
            Long movieId,
            int rankNo,
            double finalScore,
            String displayTitle,
            String displaySubtitle,
            String movieCode,
            String posterImageUrl,
            String reasonSummary
    ) {
        private BlockMovie toBlockMovie() {
            return new BlockMovie(rankNo, movieCode, displayTitle, displaySubtitle, posterImageUrl, reasonSummary);
        }
    }

    private record InternalBlock(
            String key,
            String title,
            String description,
            List<RankedMovie> items
    ) {
        private RecommendationBlock toExternalBlock() {
            return new RecommendationBlock(
                    key,
                    title,
                    description,
                    items.stream().map(RankedMovie::toBlockMovie).toList()
            );
        }
    }

    private record PersonalizedCandidate(
            RankedMovie movie,
            String dominantGenre,
            String dominantTag,
            String dominantDirector,
            String reasonPattern,
            Set<String> thematicBlockKeys
    ) {
    }

    public record BlockMovie(
            int rankNo,
            String movieCode,
            String title,
            String subtitle,
            String posterImageUrl,
            String reasonSummary
    ) {
    }

    public record RecommendationBlock(
            String key,
            String title,
            String description,
            List<BlockMovie> items
    ) {
    }

    public record RecommendationBlockResponse(
            Long userId,
            int sliceLimit,
            int itemLimit,
            int blockCount,
            List<RecommendationBlock> blocks
    ) {
    }
}
