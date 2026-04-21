package com.cinematch;

import javax.sql.DataSource;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Component;

@Component
@Order(0)
@ConditionalOnProperty(value = "app.schema-bootstrap.enabled", havingValue = "true", matchIfMissing = true)
public class SchemaBootstrapRunner implements ApplicationRunner {

    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;

    public SchemaBootstrapRunner(JdbcTemplate jdbcTemplate, DataSource dataSource) {
        this.jdbcTemplate = jdbcTemplate;
        this.dataSource = dataSource;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (hasMovieTable()) {
            return;
        }

        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.setContinueOnError(false);
        populator.setIgnoreFailedDrops(true);
        populator.addScripts(
                new ClassPathResource("schema.sql"),
                new ClassPathResource("data.sql")
        );
        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    private boolean hasMovieTable() {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = 'MOVIE'
                """, Integer.class);
        return count != null && count > 0;
    }
}
