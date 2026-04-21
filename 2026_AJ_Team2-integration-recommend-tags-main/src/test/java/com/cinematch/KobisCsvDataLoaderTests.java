package com.cinematch;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "kobis.poster-fetch.enabled=false")
class KobisCsvDataLoaderTests {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void csvDataIsLoadedIntoNormalizedTables() {
        Integer movieCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM movie", Integer.class);
        Integer genreCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM genre", Integer.class);
        Integer actorLinkCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM movie_actor", Integer.class);
        Integer companyLinkCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM movie_company", Integer.class);

        assertThat(movieCount).isEqualTo(50);
        assertThat(genreCount).isGreaterThan(0);
        assertThat(actorLinkCount).isGreaterThan(50);
        assertThat(companyLinkCount).isGreaterThan(50);
    }

    @Test
    void multiValueColumnsAreSplitIntoReferenceTables() {
        Integer genreLinksForMyungryang = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM movie_genre mg
                JOIN movie m ON m.id = mg.movie_id
                WHERE m.movie_cd = '20129370'
                """, Integer.class);

        Integer actorLinksForAvengers = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM movie_actor ma
                JOIN movie m ON m.id = ma.movie_id
                WHERE m.movie_cd = '20184889'
                """, Integer.class);

        Integer auditLinksForAvatar = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM movie_audit mat
                JOIN movie m ON m.id = mat.movie_id
                WHERE m.movie_cd = '20090834'
                """, Integer.class);

        assertThat(genreLinksForMyungryang).isEqualTo(2);
        assertThat(actorLinksForAvengers).isEqualTo(10);
        assertThat(auditLinksForAvatar).isEqualTo(2);
    }
}
