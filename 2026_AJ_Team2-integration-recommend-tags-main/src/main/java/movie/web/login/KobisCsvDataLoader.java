package movie.web.login;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Order(1)
@ConditionalOnProperty(value = "app.csv-init.enabled", havingValue = "true")
public class KobisCsvDataLoader implements ApplicationRunner {

    private static final String CSV_PATH = "data/kobis_boxoffice_top50_movie_details.csv";
    private static final Pattern COMPANY_PATTERN = Pattern.compile("^(.*?)(?:\\s*\\[(.+)])?$");
    private static final DateTimeFormatter BASIC_DATE = DateTimeFormatter.BASIC_ISO_DATE;

    private final JdbcTemplate jdbcTemplate;

    public KobisCsvDataLoader(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) throws Exception {
        Integer movieCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM movie", Integer.class);
        if (movieCount != null && movieCount > 0) {
            return;
        }
        loadCsv();
    }

    private void loadCsv() throws IOException {
        Map<String, Long> movieTypeIds = new LinkedHashMap<>();
        Map<String, Long> productionStatusIds = new LinkedHashMap<>();
        Map<String, Long> nationIds = new LinkedHashMap<>();
        Map<String, Long> genreIds = new LinkedHashMap<>();
        Map<String, Long> personIds = new LinkedHashMap<>();
        Map<String, Long> companyIds = new LinkedHashMap<>();
        Map<String, Long> watchGradeIds = new LinkedHashMap<>();
        Map<String, Long> auditIds = new LinkedHashMap<>();

        try (Reader reader = new BufferedReader(new InputStreamReader(
                BOMInputStream.builder()
                        .setInputStream(new ClassPathResource(CSV_PATH).getInputStream())
                        .get(),
                StandardCharsets.UTF_8));
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setTrim(true)
                     .build()
                     .parse(reader)) {

            for (CSVRecord record : parser) {
                long movieId = insertMovie(record, movieTypeIds, productionStatusIds, nationIds);
                insertGenres(movieId, splitPipe(record.get("genreNm")), genreIds);
                insertPeople(movieId, splitPipe(record.get("directorNm")), personIds, "movie_director");
                insertPeople(movieId, splitPipe(record.get("actorNm")), personIds, "movie_actor");
                insertCompanies(movieId, splitPipe(record.get("companyNm")), companyIds);
                insertAudits(movieId, splitPipe(record.get("auditNo")), splitPipe(record.get("watchGradeNm")),
                        auditIds, watchGradeIds);
            }
        }
    }

    private long insertMovie(CSVRecord record, Map<String, Long> movieTypeIds,
                             Map<String, Long> productionStatusIds, Map<String, Long> nationIds) {
        Long movieTypeId = resolveLookupId(movieTypeIds, "movie_type", "name", record.get("typeNm"));
        Long productionStatusId = resolveLookupId(productionStatusIds, "production_status", "name", record.get("prdtStatNm"));
        Long nationId = resolveLookupId(nationIds, "nation", "name", record.get("nationNm"));

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO movie (
                        ranking, movie_cd, movie_name, movie_name_en, movie_name_original,
                        box_office_open_date, movie_info_open_date, production_year, show_time,
                        movie_type_id, production_status_id, nation_id, poster_image_url, box_office_sales_acc,
                        box_office_audi_acc, box_office_scrn_cnt, box_office_show_cnt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, Statement.RETURN_GENERATED_KEYS);
            statement.setObject(1, parseInteger(record.get("rank")));
            statement.setString(2, blankToNull(record.get("movieCd")));
            statement.setString(3, blankToNull(record.get("movieNm")));
            statement.setString(4, blankToNull(record.get("movieNmEn")));
            statement.setString(5, blankToNull(record.get("movieNmOg")));
            statement.setDate(6, toSqlDate(parseDate(record.get("boxOfficeOpenDt"), false)));
            statement.setDate(7, toSqlDate(parseDate(record.get("movieInfoOpenDt"), true)));
            statement.setObject(8, parseInteger(record.get("prdtYear")));
            statement.setObject(9, parseInteger(record.get("showTm")));
            statement.setObject(10, movieTypeId);
            statement.setObject(11, productionStatusId);
            statement.setObject(12, nationId);
            statement.setString(13, null);
            statement.setObject(14, parseLong(record.get("boxOfficeSalesAcc")));
            statement.setObject(15, parseLong(record.get("boxOfficeAudiAcc")));
            statement.setObject(16, parseInteger(record.get("boxOfficeScrnCnt")));
            statement.setObject(17, parseInteger(record.get("boxOfficeShowCnt")));
            return statement;
        }, keyHolder);
        return keyHolder.getKeyAs(Long.class);
    }

    private void insertGenres(long movieId, List<String> genres, Map<String, Long> genreIds) {
        List<String> distinctGenres = distinctValues(genres);
        for (int i = 0; i < distinctGenres.size(); i++) {
            Long genreId = resolveLookupId(genreIds, "genre", "name", distinctGenres.get(i));
            jdbcTemplate.update("INSERT INTO movie_genre (movie_id, genre_id, display_order) VALUES (?, ?, ?)",
                    movieId, genreId, i + 1);
        }
    }

    private void insertPeople(long movieId, List<String> people, Map<String, Long> personIds, String tableName) {
        List<String> distinctPeople = distinctValues(people);
        for (int i = 0; i < distinctPeople.size(); i++) {
            Long personId = resolveLookupId(personIds, "person", "name", distinctPeople.get(i));
            jdbcTemplate.update("INSERT INTO " + tableName + " (movie_id, person_id, display_order) VALUES (?, ?, ?)",
                    movieId, personId, i + 1);
        }
    }

    private void insertCompanies(long movieId, List<String> companies, Map<String, Long> companyIds) {
        List<String> distinctCompanies = distinctValues(companies);
        for (int i = 0; i < distinctCompanies.size(); i++) {
            CompanyEntry companyEntry = parseCompany(distinctCompanies.get(i));
            Long companyId = resolveLookupId(companyIds, "company", "name", companyEntry.name());
            jdbcTemplate.update("""
                    INSERT INTO movie_company (movie_id, company_id, company_role, display_order)
                    VALUES (?, ?, ?, ?)
                    """, movieId, companyId, companyEntry.role(), i + 1);
        }
    }

    private void insertAudits(long movieId, List<String> auditNumbers, List<String> watchGrades,
                              Map<String, Long> auditIds, Map<String, Long> watchGradeIds) {
        int size = Math.max(auditNumbers.size(), watchGrades.size());
        for (int i = 0; i < size; i++) {
            String auditNo = i < auditNumbers.size() ? auditNumbers.get(i) : null;
            String watchGrade = i < watchGrades.size() ? watchGrades.get(i) : null;
            Long auditId = resolveLookupId(auditIds, "audit", "audit_no", auditNo);
            Long watchGradeId = resolveLookupId(watchGradeIds, "watch_grade", "name", watchGrade);
            if (auditId == null && watchGradeId == null) {
                continue;
            }
            jdbcTemplate.update("""
                    INSERT INTO movie_audit (movie_id, audit_id, watch_grade_id, display_order)
                    VALUES (?, ?, ?, ?)
                    """, movieId, auditId, watchGradeId, i + 1);
        }
    }

    private Long resolveLookupId(Map<String, Long> cache, String tableName, String columnName, String rawValue) {
        String value = blankToNull(rawValue);
        if (value == null) {
            return null;
        }
        return cache.computeIfAbsent(value, key -> insertLookup(tableName, columnName, key));
    }

    private Long insertLookup(String tableName, String columnName, String value) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO " + tableName + " (" + columnName + ") VALUES (?)",
                    Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, value);
            return statement;
        }, keyHolder);
        return keyHolder.getKeyAs(Long.class);
    }

    private List<String> splitPipe(String rawValue) {
        List<String> values = new ArrayList<>();
        String value = blankToNull(rawValue);
        if (value == null) {
            return values;
        }
        for (String token : value.split("\\|")) {
            String trimmed = blankToNull(token);
            if (trimmed != null) {
                values.add(trimmed);
            }
        }
        return values;
    }

    private List<String> distinctValues(List<String> values) {
        Set<String> distinctValues = new LinkedHashSet<>(values);
        return new ArrayList<>(distinctValues);
    }

    private CompanyEntry parseCompany(String rawValue) {
        Matcher matcher = COMPANY_PATTERN.matcher(rawValue);
        if (!matcher.matches()) {
            return new CompanyEntry(rawValue, null);
        }
        return new CompanyEntry(blankToNull(matcher.group(1)), blankToNull(matcher.group(2)));
    }

    private Integer parseInteger(String rawValue) {
        String value = blankToNull(rawValue);
        return value == null ? null : Integer.valueOf(value);
    }

    private Long parseLong(String rawValue) {
        String value = blankToNull(rawValue);
        return value == null ? null : Long.valueOf(value);
    }

    private LocalDate parseDate(String rawValue, boolean basicFormat) {
        String value = blankToNull(rawValue);
        if (value == null) {
            return null;
        }
        return basicFormat ? LocalDate.parse(value, BASIC_DATE) : LocalDate.parse(value);
    }

    private Date toSqlDate(LocalDate value) {
        return value == null ? null : Date.valueOf(value);
    }

    private String blankToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private record CompanyEntry(String name, String role) {
    }
}
