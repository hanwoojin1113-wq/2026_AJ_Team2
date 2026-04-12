package movie.web.login.movie;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "tmdb_movie_raw")
public class TmdbMovieRaw {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "tmdb_movie_id", unique = true, nullable = false)
    private Long tmdbMovieId;

    @Lob
    @Column(name = "payload", columnDefinition = "CLOB")
    private String payload;

    @Column(name = "imported_at")
    private LocalDateTime importedAt;

    protected TmdbMovieRaw() {
    }

    public TmdbMovieRaw(Long tmdbMovieId, String payload, LocalDateTime importedAt) {
        this.tmdbMovieId = tmdbMovieId;
        this.payload = payload;
        this.importedAt = importedAt;
    }

    public void updatePayload(String payload, LocalDateTime importedAt) {
        this.payload = payload;
        this.importedAt = importedAt;
    }
}
