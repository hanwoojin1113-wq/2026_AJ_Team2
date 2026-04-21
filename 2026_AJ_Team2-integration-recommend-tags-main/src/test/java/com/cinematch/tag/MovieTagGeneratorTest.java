package com.cinematch.tag;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MovieTagGeneratorTest {

    private final MovieTagGenerator generator =
            new MovieTagGenerator(new DefaultRecommendationTagRules(), new TagScoreCalculator());

    @Test
    void zombieMovieGetsZombieTag() {
        MovieTagInput input = new MovieTagInput(
                1L,
                "#Alive",
                98,
                2020,
                Set.of(GenreNames.HORROR, GenreNames.SF, GenreNames.ACTION),
                Set.of("zombie", "zombie apocalypse", "survival horror")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("zombie");
    }

    @Test
    void familyAnimationDoesNotGetZombieTag() {
        MovieTagInput input = new MovieTagInput(
                2L,
                "Animated Monster Town",
                92,
                2024,
                Set.of(GenreNames.ANIMATION, GenreNames.FAMILY),
                Set.of("zombie", "talking animal")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .doesNotContain("zombie");
    }

    @Test
    void romanceMovieGetsRomanticAndWithPartnerTags() {
        MovieTagInput input = new MovieTagInput(
                3L,
                "First Love",
                118,
                2019,
                Set.of(GenreNames.ROMANCE, GenreNames.DRAMA),
                Set.of("first love", "romance", "romantic")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("romantic", "with_partner");
    }

    @Test
    void whodunitMovieGetsInvestigationTag() {
        MovieTagInput input = new MovieTagInput(
                31L,
                "Murder on the Line",
                114,
                2024,
                Set.of(GenreNames.MYSTERY, GenreNames.THRILLER),
                Set.of("murder mystery", "whodunit")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("investigation");
    }

    @Test
    void detectiveMovieGetsInvestigationTag() {
        MovieTagInput input = new MovieTagInput(
                4L,
                "Case Zero",
                121,
                2022,
                Set.of(GenreNames.MYSTERY, GenreNames.CRIME, GenreNames.THRILLER),
                Set.of("detective", "investigation", "police", "murder")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("investigation");
    }

    @Test
    void familyMovieGetsWithFamilyTagFromFamilySignals() {
        MovieTagInput input = new MovieTagInput(
                41L,
                "Holiday Animal Adventure",
                101,
                2024,
                Set.of(GenreNames.ANIMATION, GenreNames.FAMILY, GenreNames.COMEDY),
                Set.of("holiday", "talking animal", "playful", "3d animation")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("with_family");
    }

    @Test
    void romanceMovieGetsWithPartnerFromRelationshipSignals() {
        MovieTagInput input = new MovieTagInput(
                44L,
                "Holiday Date",
                109,
                2024,
                Set.of(GenreNames.ROMANCE, GenreNames.COMEDY),
                Set.of("falling in love", "relationship", "romcom")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("with_partner");
    }

    @Test
    void emotionalDramaGetsEmotionalTag() {
        MovieTagInput input = new MovieTagInput(
                42L,
                "Letters Home",
                124,
                2023,
                Set.of(GenreNames.DRAMA, GenreNames.FAMILY),
                Set.of("tearjerker", "heartfelt", "family")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("emotional");
    }

    @Test
    void comfortingDramaGetsHealingTag() {
        MovieTagInput input = new MovieTagInput(
                421L,
                "Quiet Winter",
                112,
                2024,
                Set.of(GenreNames.DRAMA, GenreNames.FAMILY),
                Set.of("comforting", "second chance", "found family", "slice of life")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("healing");
    }

    @Test
    void youthDramaGetsComingOfAgeTag() {
        MovieTagInput input = new MovieTagInput(
                45L,
                "School Days",
                112,
                2024,
                Set.of(GenreNames.DRAMA),
                Set.of("teenager", "youth", "school", "friendship")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("coming_of_age");
    }

    @Test
    void friendshipMovieGetsFriendshipTag() {
        MovieTagInput input = new MovieTagInput(
                49L,
                "Summer Club",
                110,
                2024,
                Set.of(GenreNames.DRAMA, GenreNames.FAMILY),
                Set.of("friendship", "childhood friends", "teamwork")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("friendship");
    }

    @Test
    void disappearanceThrillerGetsMysteryTag() {
        MovieTagInput input = new MovieTagInput(
                491L,
                "Hidden Lake",
                118,
                2024,
                Set.of(GenreNames.THRILLER, GenreNames.DRAMA),
                Set.of("disappearance", "missing person", "secret")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("mystery");
    }

    @Test
    void survivalThrillerGetsSurvivalTag() {
        MovieTagInput input = new MovieTagInput(
                492L,
                "Last Signal",
                109,
                2024,
                Set.of(GenreNames.THRILLER, GenreNames.SF),
                Set.of("survival", "outbreak", "trapped")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("survival");
    }

    @Test
    void revengeThrillerGetsRevengeTag() {
        MovieTagInput input = new MovieTagInput(
                493L,
                "Cold Retribution",
                117,
                2024,
                Set.of(GenreNames.THRILLER, GenreNames.CRIME),
                Set.of("revenge", "vigilante", "murder")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("revenge");
    }

    @Test
    void familyAnimationWithoutComedyDoesNotAutomaticallyGetFunnyTag() {
        MovieTagInput input = new MovieTagInput(
                43L,
                "Magic Snow Kingdom",
                104,
                2024,
                Set.of(GenreNames.ANIMATION, GenreNames.FAMILY, GenreNames.FANTASY),
                Set.of("cheerful", "magic", "holiday")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .doesNotContain("funny");
    }

    @Test
    void comedyMovieGetsFunnyTagFromComedySignals() {
        MovieTagInput input = new MovieTagInput(
                46L,
                "Buddy Trouble",
                103,
                2024,
                Set.of(GenreNames.COMEDY, GenreNames.CRIME),
                Set.of("buddy comedy", "witty", "satire")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("funny");
    }

    @Test
    void familyAnimationComedyDoesNotAutomaticallyGetFunnyTag() {
        MovieTagInput input = new MovieTagInput(
                47L,
                "Animal Squad",
                99,
                2024,
                Set.of(GenreNames.COMEDY, GenreNames.ANIMATION, GenreNames.FAMILY),
                Set.of("talking animal", "3d animation", "playful")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .doesNotContain("funny");
    }

    @Test
    void fairyTaleAnimationDoesNotAutomaticallyGetSpectacleTag() {
        MovieTagInput input = new MovieTagInput(
                48L,
                "Snow Kingdom",
                102,
                2024,
                Set.of(GenreNames.FANTASY, GenreNames.ANIMATION, GenreNames.FAMILY),
                Set.of("magic", "wizard", "based on fairy tale", "3d animation")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .doesNotContain("spectacle");
    }

    @Test
    void longRuntimeMovieGetsLongRunningTag() {
        MovieTagInput input = new MovieTagInput(
                5L,
                "Epic Chronicle",
                161,
                2021,
                Set.of(GenreNames.DRAMA, GenreNames.WAR),
                Set.of("historical fiction")
        );

        assertThat(generator.generate(input))
                .extracting(MovieTagResult::tagName)
                .contains("long_running");
    }
}
