package com.cinematch.tag;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class TagRule {

    private final RecommendationTag tag;
    private final Map<String, Integer> positiveGenreWeights;
    private final Map<String, Integer> negativeGenreWeights;
    private final Map<String, Integer> positiveKeywordWeights;
    private final Map<String, Integer> negativeKeywordWeights;
    private final Set<String> requiredGenresAny;
    private final Set<String> hardExcludedGenres;
    private final Set<String> requiredKeywordsAny;
    private final Set<String> hardExcludedKeywords;
    private final List<RuntimeWeight> runtimeWeights;

    private TagRule(Builder builder) {
        this.tag = builder.tag;
        this.positiveGenreWeights = Map.copyOf(builder.positiveGenreWeights);
        this.negativeGenreWeights = Map.copyOf(builder.negativeGenreWeights);
        this.positiveKeywordWeights = Map.copyOf(builder.positiveKeywordWeights);
        this.negativeKeywordWeights = Map.copyOf(builder.negativeKeywordWeights);
        this.requiredGenresAny = Set.copyOf(builder.requiredGenresAny);
        this.hardExcludedGenres = Set.copyOf(builder.hardExcludedGenres);
        this.requiredKeywordsAny = Set.copyOf(builder.requiredKeywordsAny);
        this.hardExcludedKeywords = Set.copyOf(builder.hardExcludedKeywords);
        this.runtimeWeights = List.copyOf(builder.runtimeWeights);
    }

    public RecommendationTag tag() {
        return tag;
    }

    public Map<String, Integer> positiveGenreWeights() {
        return positiveGenreWeights;
    }

    public Map<String, Integer> negativeGenreWeights() {
        return negativeGenreWeights;
    }

    public Map<String, Integer> positiveKeywordWeights() {
        return positiveKeywordWeights;
    }

    public Map<String, Integer> negativeKeywordWeights() {
        return negativeKeywordWeights;
    }

    public Set<String> requiredGenresAny() {
        return requiredGenresAny;
    }

    public Set<String> hardExcludedGenres() {
        return hardExcludedGenres;
    }

    public Set<String> requiredKeywordsAny() {
        return requiredKeywordsAny;
    }

    public Set<String> hardExcludedKeywords() {
        return hardExcludedKeywords;
    }

    public List<RuntimeWeight> runtimeWeights() {
        return runtimeWeights;
    }

    public static Builder builder(RecommendationTag tag) {
        return new Builder(tag);
    }

    public static final class Builder {
        private final RecommendationTag tag;
        private final Map<String, Integer> positiveGenreWeights = new LinkedHashMap<>();
        private final Map<String, Integer> negativeGenreWeights = new LinkedHashMap<>();
        private final Map<String, Integer> positiveKeywordWeights = new LinkedHashMap<>();
        private final Map<String, Integer> negativeKeywordWeights = new LinkedHashMap<>();
        private final Set<String> requiredGenresAny = new LinkedHashSet<>();
        private final Set<String> hardExcludedGenres = new LinkedHashSet<>();
        private final Set<String> requiredKeywordsAny = new LinkedHashSet<>();
        private final Set<String> hardExcludedKeywords = new LinkedHashSet<>();
        private final List<RuntimeWeight> runtimeWeights = new ArrayList<>();

        private Builder(RecommendationTag tag) {
            this.tag = tag;
        }

        public Builder positiveGenre(String genreName, int weight) {
            positiveGenreWeights.put(genreName, weight);
            return this;
        }

        public Builder negativeGenre(String genreName, int weight) {
            negativeGenreWeights.put(genreName, weight);
            return this;
        }

        public Builder positiveKeyword(String keyword, int weight) {
            positiveKeywordWeights.put(normalizeKeyword(keyword), weight);
            return this;
        }

        public Builder negativeKeyword(String keyword, int weight) {
            negativeKeywordWeights.put(normalizeKeyword(keyword), weight);
            return this;
        }

        public Builder requiredGenre(String genreName) {
            requiredGenresAny.add(genreName);
            return this;
        }

        public Builder hardExcludedGenre(String genreName) {
            hardExcludedGenres.add(genreName);
            return this;
        }

        public Builder requiredKeyword(String keyword) {
            requiredKeywordsAny.add(normalizeKeyword(keyword));
            return this;
        }

        public Builder hardExcludedKeyword(String keyword) {
            hardExcludedKeywords.add(normalizeKeyword(keyword));
            return this;
        }

        public Builder runtimeWeight(int minimumMinutes, int weight) {
            runtimeWeights.add(new RuntimeWeight(minimumMinutes, weight));
            return this;
        }

        public TagRule build() {
            return new TagRule(this);
        }

        private String normalizeKeyword(String keyword) {
            return keyword.toLowerCase(Locale.ROOT).trim();
        }
    }

    public record RuntimeWeight(int minimumMinutesInclusive, int weight) {
    }
}
