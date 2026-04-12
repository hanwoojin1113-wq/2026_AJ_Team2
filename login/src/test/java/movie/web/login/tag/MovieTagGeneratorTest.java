package movie.web.login.tag;

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
