package movie.web.login.movie;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "tmdb_movie")
public class TmdbMovie {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "tmdb_movie_id", unique = true, nullable = false)
    private Long tmdbMovieId;

    @Column(name = "title", nullable = false)
    private String title;

    @Column(name = "original_title")
    private String originalTitle;

    @Lob
    @Column(name = "overview", columnDefinition = "CLOB")
    private String overview;

    @Column(name = "release_date")
    private LocalDate releaseDate;

    @Column(name = "poster_path")
    private String posterPath;

    @Column(name = "backdrop_path")
    private String backdropPath;

    @Column(name = "popularity")
    private Double popularity;

    @Column(name = "vote_average")
    private Double voteAverage;

    @Column(name = "vote_count")
    private Integer voteCount;

    @Column(name = "adult")
    private Boolean adult;

    @Column(name = "original_language")
    private String originalLanguage;

    @Column(name = "homepage")
    private String homepage;

    @Column(name = "imdb_id")
    private String imdbId;

    @Column(name = "runtime")
    private Integer runtime;

    @Column(name = "budget")
    private Long budget;

    @Column(name = "revenue")
    private Long revenue;

    @Column(name = "tmdb_status")
    private String status;

    @Column(name = "tagline")
    private String tagline;

    @Column(name = "video")
    private Boolean video;

    @Lob
    @Column(name = "belongs_to_collection_json", columnDefinition = "CLOB")
    private String belongsToCollectionJson;

    @Lob
    @Column(name = "genres_json", columnDefinition = "CLOB")
    private String genresJson;

    @Lob
    @Column(name = "production_companies_json", columnDefinition = "CLOB")
    private String productionCompaniesJson;

    @Lob
    @Column(name = "production_countries_json", columnDefinition = "CLOB")
    private String productionCountriesJson;

    @Lob
    @Column(name = "spoken_languages_json", columnDefinition = "CLOB")
    private String spokenLanguagesJson;

    @Lob
    @Column(name = "credits_json", columnDefinition = "CLOB")
    private String creditsJson;

    @Lob
    @Column(name = "keywords_json", columnDefinition = "CLOB")
    private String keywordsJson;

    @Lob
    @Column(name = "videos_json", columnDefinition = "CLOB")
    private String videosJson;

    @Lob
    @Column(name = "release_dates_json", columnDefinition = "CLOB")
    private String releaseDatesJson;

    @Lob
    @Column(name = "watch_providers_json", columnDefinition = "CLOB")
    private String watchProvidersJson;

    @Lob
    @Column(name = "images_json", columnDefinition = "CLOB")
    private String imagesJson;

    @Column(name = "imported_at")
    private LocalDateTime importedAt;

    protected TmdbMovie() {
    }

    public TmdbMovie(
            Long tmdbMovieId,
            String title,
            String originalTitle,
            String overview,
            LocalDate releaseDate,
            String posterPath,
            String backdropPath,
            Double popularity,
            Double voteAverage,
            Integer voteCount,
            Boolean adult,
            String originalLanguage,
            String homepage,
            String imdbId,
            Integer runtime,
            Long budget,
            Long revenue,
            String status,
            String tagline,
            Boolean video,
            String belongsToCollectionJson,
            String genresJson,
            String productionCompaniesJson,
            String productionCountriesJson,
            String spokenLanguagesJson,
            String creditsJson,
            String keywordsJson,
            String videosJson,
            String releaseDatesJson,
            String watchProvidersJson,
            String imagesJson,
            LocalDateTime importedAt
    ) {
        this.tmdbMovieId = tmdbMovieId;
        update(
                title,
                originalTitle,
                overview,
                releaseDate,
                posterPath,
                backdropPath,
                popularity,
                voteAverage,
                voteCount,
                adult,
                originalLanguage,
                homepage,
                imdbId,
                runtime,
                budget,
                revenue,
                status,
                tagline,
                video,
                belongsToCollectionJson,
                genresJson,
                productionCompaniesJson,
                productionCountriesJson,
                spokenLanguagesJson,
                creditsJson,
                keywordsJson,
                videosJson,
                releaseDatesJson,
                watchProvidersJson,
                imagesJson,
                importedAt
        );
    }

    public void update(
            String title,
            String originalTitle,
            String overview,
            LocalDate releaseDate,
            String posterPath,
            String backdropPath,
            Double popularity,
            Double voteAverage,
            Integer voteCount,
            Boolean adult,
            String originalLanguage,
            String homepage,
            String imdbId,
            Integer runtime,
            Long budget,
            Long revenue,
            String status,
            String tagline,
            Boolean video,
            String belongsToCollectionJson,
            String genresJson,
            String productionCompaniesJson,
            String productionCountriesJson,
            String spokenLanguagesJson,
            String creditsJson,
            String keywordsJson,
            String videosJson,
            String releaseDatesJson,
            String watchProvidersJson,
            String imagesJson,
            LocalDateTime importedAt
    ) {
        this.title = title;
        this.originalTitle = originalTitle;
        this.overview = overview;
        this.releaseDate = releaseDate;
        this.posterPath = posterPath;
        this.backdropPath = backdropPath;
        this.popularity = popularity;
        this.voteAverage = voteAverage;
        this.voteCount = voteCount;
        this.adult = adult;
        this.originalLanguage = originalLanguage;
        this.homepage = homepage;
        this.imdbId = imdbId;
        this.runtime = runtime;
        this.budget = budget;
        this.revenue = revenue;
        this.status = status;
        this.tagline = tagline;
        this.video = video;
        this.belongsToCollectionJson = belongsToCollectionJson;
        this.genresJson = genresJson;
        this.productionCompaniesJson = productionCompaniesJson;
        this.productionCountriesJson = productionCountriesJson;
        this.spokenLanguagesJson = spokenLanguagesJson;
        this.creditsJson = creditsJson;
        this.keywordsJson = keywordsJson;
        this.videosJson = videosJson;
        this.releaseDatesJson = releaseDatesJson;
        this.watchProvidersJson = watchProvidersJson;
        this.imagesJson = imagesJson;
        this.importedAt = importedAt;
    }
}
