package com.cinematch.tastemap;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import static java.util.Map.entry;

@Service
public class TasteMapService {

    private final JdbcTemplate jdbcTemplate;

    // 장르별 [X가중치, Y가중치]
    // X: 음수=강렬함(액션/스릴러/공포), 양수=가벼움(코미디/로맨스/가족)
    // Y: 양수=상상/환상(SF/판타지/모험), 음수=현실/일상(다큐/역사/드라마)
    // 비슷한 장르는 비슷한 좌표를 갖도록 설계
    private static final Map<String, double[]> GENRE_WEIGHTS = Map.ofEntries(
        // ── 강렬함 계열 (X 음수) ──────────────────────────────────────────────
        entry("액션",       new double[]{-1.0,  0.30}),  // 강렬+약간 스펙터클
        entry("스릴러",     new double[]{-1.0, -0.25}),  // 강렬+약간 현실적 긴장
        entry("공포",       new double[]{-0.9, -0.10}),  // 강렬+중립(초자연 포함)
        entry("범죄",       new double[]{-0.70, -0.50}), // 강렬+현실적
        entry("미스터리",   new double[]{-0.60, -0.20}), // 중-강렬+약간 현실
        entry("재난",       new double[]{-0.80,  0.00}), // 강렬+중립
        entry("전쟁",       new double[]{-0.70, -0.80}), // 강렬+매우 현실적
        entry("서부",       new double[]{-0.50,  0.10}), // 중-강렬+약간 모험

        // ── 상상/환상 계열 (Y 양수) ──────────────────────────────────────────
        entry("SF",         new double[]{-0.30,  1.00}), // 약간 강렬+매우 환상적
        entry("판타지",     new double[]{ 0.20,  1.00}), // 약간 가벼움+매우 환상적
        entry("모험",       new double[]{ 0.30,  0.80}), // 가벼움+환상적 (판타지에 근접)
        entry("애니메이션", new double[]{ 0.40,  0.70}), // 가벼움+환상 (모험에 근접)

        // ── 가벼움 계열 (X 양수) ─────────────────────────────────────────────
        entry("코미디",     new double[]{ 1.00,  0.10}), // 매우 가벼움+중립
        entry("가족",       new double[]{ 0.60,  0.30}), // 가벼움+약간 환상(동화적)
        entry("뮤지컬",     new double[]{ 0.70,  0.40}), // 가벼움+약간 환상(무대)
        entry("공연",       new double[]{ 0.60,  0.50}), // 가벼움+약간 환상 (뮤지컬에 근접)
        entry("음악",       new double[]{ 0.50, -0.10}), // 가벼움+중립

        // ── 현실/일상 계열 (Y 음수) ──────────────────────────────────────────
        entry("로맨스",     new double[]{ 0.90, -0.50}), // 가벼움+현실적
        entry("멜로",       new double[]{ 0.80, -0.60}), // 가벼움+현실 (로맨스에 근접)
        entry("드라마",     new double[]{ 0.00, -0.70}), // 중립+현실적 (중앙 하단)
        entry("스포츠",     new double[]{ 0.20, -0.30}), // 약간 가벼움+약간 현실
        entry("다큐멘터리", new double[]{-0.10, -1.00}), // 중립+매우 현실적
        entry("역사",       new double[]{-0.30, -0.90}), // 약간 강렬+매우 현실
        entry("사극",       new double[]{-0.20, -0.80}), // 약간 강렬+현실 (역사에 근접)
        entry("전기",       new double[]{-0.10, -0.80})  // 중립+현실 (역사에 근접)
    );

    public TasteMapService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> getMapNodes(Long currentUserId) {
        // 1. 활동 기반 장르 점수 집계 (인생영화×5 / 좋아요×3 / 시청×1)
        Map<Long, Map<String, Double>> genreScores = new HashMap<>();

        jdbcTemplate.query("""
                SELECT src.user_id, g.name AS genre_name, SUM(src.weight) AS score
                FROM (
                    SELECT user_id, movie_id, 5.0 AS weight FROM user_movie_life
                    UNION ALL
                    SELECT user_id, movie_id, 3.0 FROM user_movie_like WHERE liked = TRUE
                    UNION ALL
                    SELECT user_id, movie_id, 1.0 FROM user_movie_watched WHERE status = 'WATCHED'
                ) src
                JOIN movie_genre mg ON mg.movie_id = src.movie_id
                JOIN genre g        ON g.id = mg.genre_id
                GROUP BY src.user_id, g.name
                """, rs -> {
            Long uid   = rs.getLong("user_id");
            String genre = rs.getString("genre_name");
            double score = rs.getDouble("score");
            genreScores.computeIfAbsent(uid, k -> new HashMap<>()).put(genre, score);
        });

        if (genreScores.isEmpty()) return List.of();

        // 2. 이중축 좌표 계산
        Map<Long, double[]> rawCoords = new HashMap<>();
        for (Map.Entry<Long, Map<String, Double>> e : genreScores.entrySet()) {
            Map<String, Double> g = e.getValue();
            rawCoords.put(e.getKey(), new double[]{computeX(g), computeY(g)});
        }

        // 3. 표준편차 기반 정규화 (min/max 대신 σ 사용 → 중앙 과밀 방지)
        double meanX = rawCoords.values().stream().mapToDouble(c -> c[0]).average().orElse(0);
        double meanY = rawCoords.values().stream().mapToDouble(c -> c[1]).average().orElse(0);
        double stdX  = Math.sqrt(rawCoords.values().stream().mapToDouble(c -> Math.pow(c[0] - meanX, 2)).average().orElse(1));
        double stdY  = Math.sqrt(rawCoords.values().stream().mapToDouble(c -> Math.pow(c[1] - meanY, 2)).average().orElse(1));

        // 4. 유저 정보 로드
        Set<Long> allIds = rawCoords.keySet();
        String placeholders = String.join(",", Collections.nCopies(allIds.size(), "?"));
        Map<Long, Map<String, Object>> userInfo = new HashMap<>();
        jdbcTemplate.query(
                "SELECT id, login_id, nickname, profile_image_url FROM \"USER\" WHERE id IN (" + placeholders + ")",
                rs -> {
                    Map<String, Object> info = new LinkedHashMap<>();
                    info.put("loginId", rs.getString("login_id"));
                    info.put("nickname", rs.getString("nickname"));
                    info.put("profileImageUrl", rs.getString("profile_image_url"));
                    userInfo.put(rs.getLong("id"), info);
                }, allIds.toArray());

        // 5. 팔로우 관계 로드
        Set<Long> following = new HashSet<>();
        if (currentUserId != null) {
            following.addAll(jdbcTemplate.queryForList(
                    "SELECT following_user_id FROM user_follow WHERE follower_user_id = ?",
                    Long.class, currentUserId));
        }

        // 6. 결과 조합 (±2σ → [-1, 1] 정규화)
        List<Map<String, Object>> nodes = new ArrayList<>();
        for (Long uid : allIds) {
            Map<String, Object> info = userInfo.get(uid);
            if (info == null) continue;

            double[] raw = rawCoords.get(uid);
            double nx = clamp((raw[0] - meanX) / (Math.max(stdX, 0.01) * 2));
            double ny = clamp((raw[1] - meanY) / (Math.max(stdY, 0.01) * 2));

            // 겹침 방지용 미세 지터
            double jx = ((uid * 2654435761L & 0xFF) - 128) / 128.0 * 0.03;
            double jy = (((uid * 2654435761L >> 8) & 0xFF) - 128) / 128.0 * 0.03;

            Map<String, Object> node = new LinkedHashMap<>(info);
            node.put("userId", uid);
            node.put("x", clamp(round3(nx + jx)));
            node.put("y", clamp(round3(ny + jy)));
            node.put("isMe", uid.equals(currentUserId));
            node.put("isFollowing", following.contains(uid));
            nodes.add(node);
        }

        return nodes;
    }

    private double computeX(Map<String, Double> genres) {
        double score = 0;
        for (Map.Entry<String, Double> e : genres.entrySet()) {
            double[] w = GENRE_WEIGHTS.get(e.getKey());
            if (w != null) score += e.getValue() * w[0];
        }
        return score;
    }

    private double computeY(Map<String, Double> genres) {
        double score = 0;
        for (Map.Entry<String, Double> e : genres.entrySet()) {
            double[] w = GENRE_WEIGHTS.get(e.getKey());
            if (w != null) score += e.getValue() * w[1];
        }
        return score;
    }

    private static double clamp(double v) { return Math.max(-1.0, Math.min(1.0, v)); }
    private static double round3(double v) { return Math.round(v * 1000.0) / 1000.0; }
}
