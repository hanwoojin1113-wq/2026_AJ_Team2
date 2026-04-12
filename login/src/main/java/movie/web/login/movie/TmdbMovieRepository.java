package movie.web.login.movie;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TmdbMovieRepository extends JpaRepository<TmdbMovie, Long> {
    boolean existsByTmdbMovieId(Long tmdbMovieId);

    Optional<TmdbMovie> findByTmdbMovieId(Long tmdbMovieId);
}
