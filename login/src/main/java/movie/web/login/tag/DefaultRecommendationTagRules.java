package movie.web.login.tag;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DefaultRecommendationTagRules {

    private final List<TagRule> rules = buildRules();

    public List<TagRule> rules() {
        return rules;
    }

    private List<TagRule> buildRules() {
        return List.of(
                funny(),
                tense(),
                dark(),
                emotional(),
                romantic(),
                hopeful(),
                spectacle(),
                creepy(),
                withFamily(),
                withPartner(),
                lateNight(),
                violent(),
                sad(),
                slowBurn(),
                longRunning(),
                investigation(),
                zombie(),
                disaster(),
                trueStory(),
                comingOfAge()
        );
    }

    private TagRule funny() {
        return TagRule.builder(RecommendationTag.FUNNY)
                .positiveGenre(GenreNames.COMEDY, 35)
                .positiveGenre(GenreNames.FAMILY, 10)
                .positiveGenre(GenreNames.ANIMATION, 8)
                .positiveGenre(GenreNames.ADVENTURE, 4)
                .negativeGenre(GenreNames.HORROR, 14)
                .negativeGenre(GenreNames.WAR, 8)
                .positiveKeyword("funny", 18)
                .positiveKeyword("hilarious", 18)
                .positiveKeyword("lighthearted", 14)
                .positiveKeyword("playful", 12)
                .positiveKeyword("witty", 12)
                .positiveKeyword("cheerful", 12)
                .positiveKeyword("joyful", 12)
                .positiveKeyword("absurd", 8)
                .positiveKeyword("romcom", 8)
                .negativeKeyword("murder", 16)
                .negativeKeyword("serial killer", 22)
                .negativeKeyword("grief", 14)
                .negativeKeyword("depressing", 16)
                .negativeKeyword("gore", 16)
                .build();
    }

    private TagRule tense() {
        return TagRule.builder(RecommendationTag.TENSE)
                .positiveGenre(GenreNames.THRILLER, 28)
                .positiveGenre(GenreNames.MYSTERY, 16)
                .positiveGenre(GenreNames.CRIME, 12)
                .positiveGenre(GenreNames.ACTION, 10)
                .positiveGenre(GenreNames.HORROR, 10)
                .negativeGenre(GenreNames.FAMILY, 12)
                .negativeGenre(GenreNames.ANIMATION, 10)
                .negativeGenre(GenreNames.COMEDY, 8)
                .positiveKeyword("suspenseful", 18)
                .positiveKeyword("tense", 18)
                .positiveKeyword("intense", 16)
                .positiveKeyword("investigation", 10)
                .positiveKeyword("kidnapping", 10)
                .positiveKeyword("escape", 10)
                .positiveKeyword("on the run", 10)
                .positiveKeyword("terrorism", 10)
                .negativeKeyword("cheerful", 14)
                .negativeKeyword("comforting", 14)
                .negativeKeyword("playful", 12)
                .build();
    }

    private TagRule dark() {
        return TagRule.builder(RecommendationTag.DARK)
                .positiveGenre(GenreNames.HORROR, 20)
                .positiveGenre(GenreNames.THRILLER, 18)
                .positiveGenre(GenreNames.CRIME, 12)
                .positiveGenre(GenreNames.MYSTERY, 10)
                .positiveGenre(GenreNames.WAR, 8)
                .positiveGenre(GenreNames.DRAMA, 6)
                .negativeGenre(GenreNames.FAMILY, 10)
                .negativeGenre(GenreNames.ANIMATION, 10)
                .negativeGenre(GenreNames.COMEDY, 8)
                .positiveKeyword("neo-noir", 18)
                .positiveKeyword("serial killer", 14)
                .positiveKeyword("corruption", 12)
                .positiveKeyword("grief", 14)
                .positiveKeyword("tragedy", 14)
                .positiveKeyword("depressing", 16)
                .positiveKeyword("grim", 14)
                .negativeKeyword("cheerful", 16)
                .negativeKeyword("lighthearted", 16)
                .build();
    }

    private TagRule emotional() {
        return TagRule.builder(RecommendationTag.EMOTIONAL)
                .positiveGenre(GenreNames.DRAMA, 28)
                .positiveGenre(GenreNames.ROMANCE, 12)
                .positiveGenre(GenreNames.MUSIC, 8)
                .positiveGenre(GenreNames.FAMILY, 8)
                .negativeGenre(GenreNames.HORROR, 10)
                .negativeGenre(GenreNames.ACTION, 8)
                .positiveKeyword("sentimental", 18)
                .positiveKeyword("loss of loved one", 18)
                .positiveKeyword("grief", 18)
                .positiveKeyword("friendship", 10)
                .positiveKeyword("family", 10)
                .positiveKeyword("mother daughter relationship", 10)
                .positiveKeyword("father son relationship", 10)
                .positiveKeyword("mother son relationship", 10)
                .positiveKeyword("father daughter relationship", 10)
                .positiveKeyword("comforting", 8)
                .negativeKeyword("gore", 16)
                .negativeKeyword("serial killer", 16)
                .negativeKeyword("terrorism", 12)
                .build();
    }

    private TagRule romantic() {
        return TagRule.builder(RecommendationTag.ROMANTIC)
                .requiredGenre(GenreNames.ROMANCE)
                .positiveGenre(GenreNames.ROMANCE, 35)
                .positiveGenre(GenreNames.DRAMA, 8)
                .positiveGenre(GenreNames.FANTASY, 4)
                .negativeGenre(GenreNames.HORROR, 10)
                .negativeGenre(GenreNames.CRIME, 10)
                .negativeGenre(GenreNames.WAR, 8)
                .positiveKeyword("romance", 18)
                .positiveKeyword("romantic", 16)
                .positiveKeyword("love", 12)
                .positiveKeyword("first love", 20)
                .positiveKeyword("romcom", 12)
                .positiveKeyword("adoring", 10)
                .positiveKeyword("admiring", 10)
                .negativeKeyword("murder", 16)
                .negativeKeyword("serial killer", 20)
                .negativeKeyword("gore", 20)
                .build();
    }

    private TagRule hopeful() {
        return TagRule.builder(RecommendationTag.HOPEFUL)
                .positiveGenre(GenreNames.DRAMA, 16)
                .positiveGenre(GenreNames.FAMILY, 14)
                .positiveGenre(GenreNames.ANIMATION, 10)
                .positiveGenre(GenreNames.MUSIC, 6)
                .negativeGenre(GenreNames.HORROR, 12)
                .negativeGenre(GenreNames.CRIME, 10)
                .negativeGenre(GenreNames.THRILLER, 10)
                .positiveKeyword("hopeful", 22)
                .positiveKeyword("inspirational", 18)
                .positiveKeyword("comforting", 14)
                .positiveKeyword("joyful", 12)
                .positiveKeyword("cheerful", 10)
                .positiveKeyword("friendship", 8)
                .positiveKeyword("rescue", 8)
                .negativeKeyword("depressing", 18)
                .negativeKeyword("tragedy", 16)
                .negativeKeyword("grief", 16)
                .negativeKeyword("serial killer", 20)
                .build();
    }

    private TagRule spectacle() {
        return TagRule.builder(RecommendationTag.SPECTACLE)
                .positiveGenre(GenreNames.ACTION, 20)
                .positiveGenre(GenreNames.ADVENTURE, 20)
                .positiveGenre(GenreNames.FANTASY, 16)
                .positiveGenre(GenreNames.SF, 16)
                .positiveGenre(GenreNames.ANIMATION, 8)
                .negativeGenre(GenreNames.DOCUMENTARY, 14)
                .positiveKeyword("superhero", 18)
                .positiveKeyword("wizard", 12)
                .positiveKeyword("magic", 12)
                .positiveKeyword("saving the world", 12)
                .positiveKeyword("alien invasion", 12)
                .positiveKeyword("fantasy world", 12)
                .positiveKeyword("super power", 12)
                .positiveKeyword("battle", 8)
                .positiveKeyword("monster", 8)
                .negativeKeyword("small town", 10)
                .negativeKeyword("slice of life", 14)
                .build();
    }

    private TagRule creepy() {
        return TagRule.builder(RecommendationTag.CREEPY)
                .requiredGenre(GenreNames.HORROR)
                .positiveGenre(GenreNames.HORROR, 30)
                .positiveGenre(GenreNames.MYSTERY, 14)
                .positiveGenre(GenreNames.THRILLER, 12)
                .hardExcludedGenre(GenreNames.FAMILY)
                .hardExcludedGenre(GenreNames.ANIMATION)
                .positiveKeyword("supernatural horror", 22)
                .positiveKeyword("psychological horror", 20)
                .positiveKeyword("ghost", 18)
                .positiveKeyword("exorcism", 20)
                .positiveKeyword("curse", 18)
                .positiveKeyword("demon", 18)
                .positiveKeyword("witch", 14)
                .positiveKeyword("gore", 12)
                .positiveKeyword("body horror", 18)
                .positiveKeyword("supernatural", 10)
                .negativeKeyword("cheerful", 16)
                .negativeKeyword("comforting", 16)
                .build();
    }

    private TagRule withFamily() {
        return TagRule.builder(RecommendationTag.WITH_FAMILY)
                .requiredKeyword("family")
                .requiredKeyword("friendship")
                .requiredKeyword("cheerful")
                .requiredKeyword("comforting")
                .requiredKeyword("playful")
                .requiredKeyword("talking animal")
                .requiredKeyword("christmas")
                .requiredKeyword("joyful")
                .requiredKeyword("lighthearted")
                .positiveGenre(GenreNames.FAMILY, 26)
                .positiveGenre(GenreNames.ANIMATION, 18)
                .positiveGenre(GenreNames.COMEDY, 10)
                .positiveGenre(GenreNames.MUSIC, 4)
                .positiveGenre(GenreNames.ADVENTURE, 4)
                .hardExcludedGenre(GenreNames.HORROR)
                .hardExcludedGenre(GenreNames.THRILLER)
                .hardExcludedGenre(GenreNames.CRIME)
                .hardExcludedKeyword("ghost")
                .hardExcludedKeyword("curse")
                .hardExcludedKeyword("witch")
                .hardExcludedKeyword("demon")
                .hardExcludedKeyword("psychological horror")
                .hardExcludedKeyword("supernatural horror")
                .hardExcludedKeyword("body horror")
                .positiveKeyword("family", 18)
                .positiveKeyword("friendship", 10)
                .positiveKeyword("cheerful", 10)
                .positiveKeyword("comforting", 10)
                .positiveKeyword("playful", 8)
                .positiveKeyword("talking animal", 12)
                .positiveKeyword("christmas", 8)
                .positiveKeyword("joyful", 8)
                .positiveKeyword("lighthearted", 10)
                .hardExcludedKeyword("gore")
                .hardExcludedKeyword("serial killer")
                .hardExcludedKeyword("exorcism")
                .hardExcludedKeyword("zombie")
                .negativeKeyword("grief", 14)
                .negativeKeyword("depressing", 16)
                .negativeKeyword("tragedy", 12)
                .negativeKeyword("dark comedy", 12)
                .build();
    }

    private TagRule withPartner() {
        return TagRule.builder(RecommendationTag.WITH_PARTNER)
                .requiredGenre(GenreNames.ROMANCE)
                .positiveGenre(GenreNames.ROMANCE, 32)
                .positiveGenre(GenreNames.DRAMA, 8)
                .positiveGenre(GenreNames.COMEDY, 6)
                .hardExcludedGenre(GenreNames.HORROR)
                .hardExcludedGenre(GenreNames.WAR)
                .hardExcludedGenre(GenreNames.CRIME)
                .positiveKeyword("romance", 16)
                .positiveKeyword("romantic", 14)
                .positiveKeyword("first love", 16)
                .positiveKeyword("love", 10)
                .positiveKeyword("romcom", 12)
                .positiveKeyword("christmas", 6)
                .hardExcludedKeyword("gore")
                .hardExcludedKeyword("serial killer")
                .hardExcludedKeyword("exorcism")
                .hardExcludedKeyword("zombie")
                .build();
    }

    private TagRule lateNight() {
        return TagRule.builder(RecommendationTag.LATE_NIGHT)
                .requiredGenre(GenreNames.HORROR)
                .requiredGenre(GenreNames.THRILLER)
                .requiredGenre(GenreNames.MYSTERY)
                .requiredGenre(GenreNames.CRIME)
                .positiveGenre(GenreNames.HORROR, 20)
                .positiveGenre(GenreNames.THRILLER, 18)
                .positiveGenre(GenreNames.MYSTERY, 12)
                .positiveGenre(GenreNames.CRIME, 8)
                .hardExcludedGenre(GenreNames.FAMILY)
                .hardExcludedGenre(GenreNames.ANIMATION)
                .positiveKeyword("suspenseful", 14)
                .positiveKeyword("supernatural horror", 18)
                .positiveKeyword("psychological horror", 16)
                .positiveKeyword("serial killer", 12)
                .positiveKeyword("ghost", 12)
                .positiveKeyword("investigation", 8)
                .negativeKeyword("comforting", 14)
                .negativeKeyword("cheerful", 14)
                .build();
    }

    private TagRule violent() {
        return TagRule.builder(RecommendationTag.VIOLENT)
                .positiveGenre(GenreNames.ACTION, 16)
                .positiveGenre(GenreNames.CRIME, 12)
                .positiveGenre(GenreNames.WAR, 10)
                .positiveGenre(GenreNames.HORROR, 10)
                .positiveGenre(GenreNames.THRILLER, 8)
                .negativeGenre(GenreNames.FAMILY, 12)
                .negativeGenre(GenreNames.ANIMATION, 10)
                .positiveKeyword("gore", 25)
                .positiveKeyword("violence", 18)
                .positiveKeyword("battle", 12)
                .positiveKeyword("serial killer", 12)
                .positiveKeyword("murder", 10)
                .positiveKeyword("assassin", 10)
                .positiveKeyword("terrorism", 10)
                .positiveKeyword("fight", 8)
                .build();
    }

    private TagRule sad() {
        return TagRule.builder(RecommendationTag.SAD)
                .positiveGenre(GenreNames.DRAMA, 20)
                .positiveGenre(GenreNames.ROMANCE, 8)
                .positiveGenre(GenreNames.WAR, 8)
                .positiveGenre(GenreNames.MUSIC, 6)
                .negativeGenre(GenreNames.COMEDY, 12)
                .positiveKeyword("loss of loved one", 20)
                .positiveKeyword("grief", 20)
                .positiveKeyword("tragedy", 18)
                .positiveKeyword("depressing", 18)
                .positiveKeyword("dying and death", 16)
                .positiveKeyword("death", 14)
                .positiveKeyword("orphan", 10)
                .negativeKeyword("cheerful", 20)
                .negativeKeyword("hilarious", 20)
                .negativeKeyword("lighthearted", 16)
                .build();
    }

    private TagRule slowBurn() {
        return TagRule.builder(RecommendationTag.SLOW_BURN)
                .positiveGenre(GenreNames.DRAMA, 14)
                .positiveGenre(GenreNames.MYSTERY, 8)
                .negativeGenre(GenreNames.ACTION, 12)
                .negativeGenre(GenreNames.COMEDY, 10)
                .negativeGenre(GenreNames.ANIMATION, 10)
                .runtimeWeight(150, 25)
                .runtimeWeight(130, 15)
                .runtimeWeight(120, 10)
                .positiveKeyword("reflective", 18)
                .positiveKeyword("thoughtful", 16)
                .positiveKeyword("slice of life", 18)
                .positiveKeyword("nostalgic", 10)
                .positiveKeyword("small town", 8)
                .negativeKeyword("battle", 12)
                .negativeKeyword("superhero", 12)
                .negativeKeyword("rescue", 10)
                .negativeKeyword("hilarious", 10)
                .build();
    }

    private TagRule longRunning() {
        return TagRule.builder(RecommendationTag.LONG_RUNNING)
                .runtimeWeight(160, 100)
                .build();
    }

    private TagRule investigation() {
        return TagRule.builder(RecommendationTag.INVESTIGATION)
                .requiredGenre(GenreNames.MYSTERY)
                .requiredGenre(GenreNames.CRIME)
                .requiredKeyword("detective")
                .requiredKeyword("investigation")
                .requiredKeyword("police")
                .positiveGenre(GenreNames.MYSTERY, 26)
                .positiveGenre(GenreNames.CRIME, 22)
                .positiveGenre(GenreNames.THRILLER, 8)
                .hardExcludedGenre(GenreNames.ANIMATION)
                .hardExcludedGenre(GenreNames.FAMILY)
                .positiveKeyword("detective", 22)
                .positiveKeyword("investigation", 22)
                .positiveKeyword("police", 18)
                .positiveKeyword("kidnapping", 10)
                .positiveKeyword("murder", 8)
                .positiveKeyword("serial killer", 8)
                .positiveKeyword("conspiracy", 8)
                .positiveKeyword("mystery", 8)
                .negativeKeyword("supernatural horror", 16)
                .negativeKeyword("ghost", 16)
                .negativeKeyword("demon", 16)
                .negativeKeyword("zombie", 20)
                .build();
    }

    private TagRule zombie() {
        return TagRule.builder(RecommendationTag.ZOMBIE)
                .requiredGenre(GenreNames.HORROR)
                .requiredGenre(GenreNames.THRILLER)
                .requiredGenre(GenreNames.SF)
                .requiredGenre(GenreNames.ACTION)
                .requiredKeyword("zombie")
                .requiredKeyword("zombie apocalypse")
                .requiredKeyword("undead")
                .requiredKeyword("infected")
                .positiveGenre(GenreNames.HORROR, 20)
                .positiveGenre(GenreNames.THRILLER, 8)
                .positiveGenre(GenreNames.SF, 8)
                .positiveGenre(GenreNames.ACTION, 8)
                .hardExcludedGenre(GenreNames.FAMILY)
                .hardExcludedGenre(GenreNames.ANIMATION)
                .positiveKeyword("zombie", 30)
                .positiveKeyword("zombie apocalypse", 35)
                .positiveKeyword("undead", 30)
                .positiveKeyword("infected", 25)
                .positiveKeyword("survival horror", 15)
                .positiveKeyword("post-apocalyptic future", 10)
                .positiveKeyword("outbreak", 10)
                .hardExcludedKeyword("talking animal")
                .build();
    }

    private TagRule disaster() {
        return TagRule.builder(RecommendationTag.DISASTER)
                .requiredGenre(GenreNames.ACTION)
                .requiredGenre(GenreNames.THRILLER)
                .requiredGenre(GenreNames.SF)
                .requiredGenre(GenreNames.DRAMA)
                .requiredKeyword("disaster")
                .requiredKeyword("rescue")
                .requiredKeyword("end of the world")
                .requiredKeyword("alien invasion")
                .requiredKeyword("outbreak")
                .requiredKeyword("pandemic")
                .positiveGenre(GenreNames.ACTION, 14)
                .positiveGenre(GenreNames.THRILLER, 14)
                .positiveGenre(GenreNames.SF, 10)
                .positiveGenre(GenreNames.DRAMA, 8)
                .positiveGenre(GenreNames.ADVENTURE, 8)
                .hardExcludedGenre(GenreNames.FAMILY)
                .positiveKeyword("disaster", 24)
                .positiveKeyword("rescue", 16)
                .positiveKeyword("end of the world", 20)
                .positiveKeyword("alien invasion", 18)
                .positiveKeyword("outbreak", 18)
                .positiveKeyword("pandemic", 18)
                .positiveKeyword("post-apocalyptic future", 10)
                .positiveKeyword("survival", 10)
                .negativeKeyword("cheerful", 10)
                .negativeKeyword("talking animal", 20)
                .build();
    }

    private TagRule trueStory() {
        return TagRule.builder(RecommendationTag.TRUE_STORY)
                .requiredKeyword("based on true story")
                .requiredKeyword("biography")
                .positiveGenre(GenreNames.DRAMA, 10)
                .positiveGenre(GenreNames.WAR, 6)
                .positiveGenre(GenreNames.HISTORY, 6)
                .positiveGenre(GenreNames.PERIOD, 6)
                .positiveKeyword("based on true story", 35)
                .positiveKeyword("biography", 25)
                .positiveKeyword("historical fiction", 10)
                .negativeKeyword("fantasy world", 20)
                .negativeKeyword("zombie", 20)
                .negativeKeyword("superhero", 18)
                .negativeKeyword("magic", 16)
                .build();
    }

    private TagRule comingOfAge() {
        return TagRule.builder(RecommendationTag.COMING_OF_AGE)
                .requiredGenre(GenreNames.DRAMA)
                .requiredGenre(GenreNames.ROMANCE)
                .requiredGenre(GenreNames.COMEDY)
                .requiredKeyword("coming of age")
                .requiredKeyword("high school")
                .requiredKeyword("school")
                .requiredKeyword("teenage girl")
                .positiveGenre(GenreNames.DRAMA, 18)
                .positiveGenre(GenreNames.ROMANCE, 8)
                .positiveGenre(GenreNames.COMEDY, 6)
                .positiveGenre(GenreNames.FAMILY, 6)
                .positiveGenre(GenreNames.ANIMATION, 4)
                .positiveKeyword("coming of age", 30)
                .positiveKeyword("high school", 16)
                .positiveKeyword("school", 10)
                .positiveKeyword("teenage girl", 10)
                .positiveKeyword("bullying", 8)
                .positiveKeyword("friendship", 8)
                .positiveKeyword("first love", 8)
                .positiveKeyword("slice of life", 6)
                .negativeKeyword("serial killer", 20)
                .negativeKeyword("gore", 20)
                .negativeKeyword("exorcism", 20)
                .negativeKeyword("war", 12)
                .build();
    }
}
