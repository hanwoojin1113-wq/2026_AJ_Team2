package movie.web.login.tag;

import java.util.Set;

public record MovieTagInput(
        long movieId,
        String title,
        Integer runtimeMinutes,
        Integer releaseYear,
        Set<String> genres,
        Set<String> keywords
) {
}
