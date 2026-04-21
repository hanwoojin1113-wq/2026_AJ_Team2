package com.cinematch.tag;

import java.util.Set;

public record MovieTagInput(
        long movieId,
        String title,
        Integer runtimeMinutes,
        Integer releaseYear,
        Set<String> genres,
        Set<String> keywords
) {
    /*
     * 태그 생성기는 영화 엔티티 전체를 직접 보지 않고,
     * 태깅에 필요한 최소 입력만 이 record로 전달받는다.
     * 이렇게 분리해두면 태그 로직을 테스트하기 쉽고, 데이터 소스에도 덜 의존한다.
     */
}
