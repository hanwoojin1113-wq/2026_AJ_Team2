package com.cinematch.ott;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * OTT 관련 테이블을 앱 시작 시 1회 생성한다.
 *
 * <p>{@code SchemaBootstrapRunner}(@Order(0))가 기본 스키마(USER 등)를 적용한 직후 실행되도록
 * {@code @Order(1)}을 둔다. 이렇게 startup 단계에서 DDL을 끝내두면, 추천 재계산 같은
 * 트랜잭션 핫패스에서 CREATE/ALTER가 돌며 테이블 락 경합으로 QueryTimeout이 나는 것을 방지한다.</p>
 */
@Component
@Order(1)
public class OttTableInitializer implements ApplicationRunner {

    private final OttWatchLinkService ottWatchLinkService;
    private final UserOttSubscriptionService userOttSubscriptionService;

    public OttTableInitializer(
            OttWatchLinkService ottWatchLinkService,
            UserOttSubscriptionService userOttSubscriptionService
    ) {
        this.ottWatchLinkService = ottWatchLinkService;
        this.userOttSubscriptionService = userOttSubscriptionService;
    }

    @Override
    public void run(ApplicationArguments args) {
        ottWatchLinkService.initializeTables();
        userOttSubscriptionService.initializeTable();
    }
}
