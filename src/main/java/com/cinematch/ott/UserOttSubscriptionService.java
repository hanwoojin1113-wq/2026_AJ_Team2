package com.cinematch.ott;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 사용자별 구독 OTT 정보 관리.
 *
 * <p>{@code user_ott_subscription} 테이블은 런타임에 {@code CREATE TABLE IF NOT EXISTS}로 생성한다
 * ({@link OttWatchLinkService}와 동일 전략). {@code SchemaBootstrapRunner}가 기존 DB에서 스킵되므로
 * schema.sql 추가만으로는 생성되지 않기 때문이다.</p>
 *
 * <p>OTT 식별은 {@link OttCatalog}의 {@code code}(영문 슬러그)로 저장한다.</p>
 */
@Service
public class UserOttSubscriptionService {

    private final JdbcTemplate jdbcTemplate;
    private final OttCatalog ottCatalog;
    private volatile boolean tableInitialized = false;

    public UserOttSubscriptionService(JdbcTemplate jdbcTemplate, OttCatalog ottCatalog) {
        this.jdbcTemplate = jdbcTemplate;
        this.ottCatalog = ottCatalog;
    }

    public void initializeTable() {
        // DDL은 최초 1회만 실행 (핫패스 재호출 시 테이블 락 경합 방지).
        if (tableInitialized) {
            return;
        }
        synchronized (this) {
            if (tableInitialized) {
                return;
            }
            jdbcTemplate.execute("""
                    CREATE TABLE IF NOT EXISTS user_ott_subscription (
                        user_id BIGINT NOT NULL,
                        ott_code VARCHAR(40) NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (user_id, ott_code),
                        CONSTRAINT fk_user_ott_subscription_user FOREIGN KEY (user_id) REFERENCES "USER"(id)
                    )
                    """);
            tableInitialized = true;
        }
    }

    /** 사용자가 구독한 OTT code 목록을 카탈로그 정의 순서로 반환. */
    public List<String> findSubscribedCodes(Long userId) {
        initializeTable();
        if (userId == null) {
            return List.of();
        }
        Set<String> stored = new LinkedHashSet<>(jdbcTemplate.queryForList("""
                SELECT ott_code
                FROM user_ott_subscription
                WHERE user_id = ?
                """, String.class, userId));
        List<String> ordered = new ArrayList<>();
        for (OttCatalog.OttProvider provider : ottCatalog.all()) {
            if (stored.contains(provider.code())) {
                ordered.add(provider.code());
            }
        }
        return ordered;
    }

    /** 사용자가 구독한 OTT 카탈로그 항목을 정의 순서로 반환. */
    public List<OttCatalog.OttProvider> findSubscribedProviders(Long userId) {
        return findSubscribedCodes(userId).stream()
                .map(ottCatalog::byCode)
                .filter(java.util.Optional::isPresent)
                .map(java.util.Optional::get)
                .toList();
    }

    /**
     * 구독 OTT 전체 교체. null/빈 리스트면 전부 해제(미구독).
     * 카탈로그에 없는 code는 무시(화이트리스트).
     */
    public void replaceSubscriptions(Long userId, List<String> codes) {
        initializeTable();
        if (userId == null) {
            return;
        }
        jdbcTemplate.update("DELETE FROM user_ott_subscription WHERE user_id = ?", userId);
        if (codes == null || codes.isEmpty()) {
            return;
        }
        Set<String> validCodes = new LinkedHashSet<>();
        for (String code : codes) {
            ottCatalog.byCode(code).ifPresent(provider -> validCodes.add(provider.code()));
        }
        for (String code : validCodes) {
            jdbcTemplate.update("""
                    INSERT INTO user_ott_subscription (user_id, ott_code)
                    VALUES (?, ?)
                    """, userId, code);
        }
    }
}
