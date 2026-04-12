package movie.web.login.movie;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TmdbMovieRawRepository extends JpaRepository<TmdbMovieRaw, Long> {
    boolean existsByTmdbMovieId(Long tmdbMovieId);

    Optional<TmdbMovieRaw> findByTmdbMovieId(Long tmdbMovieId);
}
