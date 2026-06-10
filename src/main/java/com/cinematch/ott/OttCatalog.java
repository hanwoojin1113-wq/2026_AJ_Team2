package com.cinematch.ott;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 서비스에 노출하는 OTT 9종의 단일 정의 출처.
 *
 * <p>OTT 식별은 크롤링 데이터({@code movie_ott_link.provider_name}) 기준으로 통일한다.
 * {@code canonicalName}은 크롤러(PROVIDER_ALIASES)가 저장하는 provider_name과 1:1로 일치해야 하며,
 * 영화의 OTT 가용성 비교는 항상 {@link OttWatchLinkService#normalizeProviderName(String)} 정규화 키로 한다.</p>
 *
 * <ul>
 *     <li>{@code code} — 폼/DB 저장 키(영문 슬러그). {@code user_ott_subscription.ott_code}</li>
 *     <li>{@code canonicalName} — {@code movie_ott_link.provider_name} 매칭용</li>
 *     <li>{@code displayName} — 화면 표시 한글 라벨</li>
 *     <li>{@code logoUrl} — 로고 정적 자산 경로(없으면 템플릿이 텍스트 폴백)</li>
 * </ul>
 */
@Component
public class OttCatalog {

    public record OttProvider(String code, String canonicalName, String displayName, String logoUrl) {
        /** 로고 자산이 깨질 때 표시할 한 글자 배지. */
        public String fallbackLabel() {
            if (displayName == null || displayName.isBlank()) {
                return "?";
            }
            return displayName.trim().substring(0, 1).toUpperCase();
        }
    }

    private static final List<OttProvider> PROVIDERS = List.of(
            new OttProvider("netflix", "Netflix", "넷플릭스", "/images/providers/netflix.png"),
            new OttProvider("tving", "TVING", "티빙", "/images/providers/tving.png"),
            new OttProvider("wavve", "Wavve", "웨이브", "/images/providers/wavve.png"),
            new OttProvider("coupang", "Coupang Play", "쿠팡플레이", "/images/providers/coupang-play.png"),
            new OttProvider("disney", "Disney+", "디즈니+", "/images/providers/disney-plus.png"),
            new OttProvider("watcha", "Watcha", "왓챠", "/images/providers/watcha.png"),
            new OttProvider("apple", "Apple TV", "애플 TV", "/images/providers/apple-tv.png"),
            new OttProvider("laftel", "Laftel", "라프텔", "/images/providers/laftel.png"),
            new OttProvider("prime", "Prime Video", "프라임 비디오", "/images/providers/prime-video.png")
    );

    private static final Map<String, OttProvider> BY_CODE = new LinkedHashMap<>();
    private static final Map<String, OttProvider> BY_NORMALIZED_NAME = new LinkedHashMap<>();

    static {
        for (OttProvider provider : PROVIDERS) {
            BY_CODE.put(provider.code(), provider);
            BY_NORMALIZED_NAME.put(OttWatchLinkService.normalizeProviderName(provider.canonicalName()), provider);
        }
    }

    public List<OttProvider> all() {
        return PROVIDERS;
    }

    public Optional<OttProvider> byCode(String code) {
        if (code == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(BY_CODE.get(code.trim()));
    }

    /** {@code movie_ott_link.provider_name} 같은 원문 OTT명을 정규화 비교로 카탈로그 항목에 매핑. */
    public Optional<OttProvider> byCanonicalName(String providerName) {
        return Optional.ofNullable(BY_NORMALIZED_NAME.get(OttWatchLinkService.normalizeProviderName(providerName)));
    }

    /** OTT 가용성 비교용 정규화 키. */
    public String normalizedKey(OttProvider provider) {
        return OttWatchLinkService.normalizeProviderName(provider.canonicalName());
    }

    /** 영화 상세 "보러가기" 로고 해석용: canonicalName → logoUrl 맵. */
    public Map<String, String> logoUrlMap() {
        Map<String, String> map = new LinkedHashMap<>();
        for (OttProvider provider : PROVIDERS) {
            if (provider.logoUrl() != null) {
                map.put(provider.canonicalName(), provider.logoUrl());
            }
        }
        return map;
    }
}
