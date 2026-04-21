package com.cinematch.chart;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 모든 {@link ChartAlgorithm} 구현체를 모아 관리하는 등록소(Registry).
 *
 * Spring이 {@code @Component} 로 등록된 ChartAlgorithm 빈을 모두 자동 주입하므로,
 * 새 알고리즘 클래스에 {@code @Component}만 추가하면 이 Registry에 자동 등록된다.
 * ChartController나 다른 서비스는 이 클래스를 통해서만 알고리즘에 접근한다.
 */
@Component
public class ChartRegistry {

    /** code → algorithm 매핑. 삽입 순서를 유지하는 LinkedHashMap 사용. */
    private final Map<String, ChartAlgorithm> algorithmMap;

    /**
     * Spring이 List<ChartAlgorithm>으로 모든 구현체를 한 번에 주입한다.
     * 알고리즘 클래스에 @Order를 달면 표시 순서를 제어할 수 있다.
     */
    public ChartRegistry(List<ChartAlgorithm> algorithms) {
        this.algorithmMap = algorithms.stream()
                .collect(Collectors.toMap(
                        ChartAlgorithm::code,
                        algorithm -> algorithm,
                        (existing, duplicate) -> existing,   // 코드 충돌 시 먼저 등록된 것 우선
                        LinkedHashMap::new
                ));
    }

    /**
     * 코드로 알고리즘을 조회한다. 존재하지 않으면 Optional.empty() 반환.
     *
     * @param code 알고리즘 고유 코드 (예: "top-sales")
     */
    public Optional<ChartAlgorithm> find(String code) {
        if (code == null || code.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(algorithmMap.get(code.trim()));
    }

    /**
     * 등록된 모든 차트를 {@link ChartEntry} 목록으로 반환한다.
     * 랭킹 목록 페이지(ranking.html)에서 카드 그리드를 렌더링할 때 사용.
     */
    public List<ChartEntry> allEntries() {
        return algorithmMap.values().stream()
                .map(ChartEntry::of)
                .toList();
    }

    /**
     * 특정 카테고리의 차트 목록만 반환한다.
     *
     * @param category 필터링할 카테고리
     */
    public List<ChartEntry> entriesByCategory(ChartCategory category) {
        if (category == null) {
            return allEntries();
        }
        return algorithmMap.values().stream()
                .filter(algorithm -> algorithm.category() == category)
                .map(ChartEntry::of)
                .toList();
    }

    /**
     * 등록된 모든 알고리즘 구현체를 반환한다.
     */
    public List<ChartAlgorithm> allAlgorithms() {
        return List.copyOf(algorithmMap.values());
    }

    /**
     * 특정 카테고리의 알고리즘 구현체만 반환한다.
     */
    public List<ChartAlgorithm> algorithmsByCategory(ChartCategory category) {
        if (category == null) return allAlgorithms();
        return algorithmMap.values().stream()
                .filter(a -> a.category() == category)
                .toList();
    }

    /**
     * 등록된 알고리즘 코드 목록을 반환한다 (디버깅·관리용).
     */
    public List<String> registeredCodes() {
        return Collections.unmodifiableList(algorithmMap.keySet().stream().toList());
    }

    /**
     * 전체 알고리즘 수를 반환한다.
     */
    public int size() {
        return algorithmMap.size();
    }
}