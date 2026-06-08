package com.cinematch.admin;

import com.cinematch.ott.OttWatchLinkService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

@RestController
@RequestMapping("/admin/ott-links")
public class OttLinkAdminController {

    private final OttWatchLinkService ottWatchLinkService;
    private final Path projectRoot;

    public OttLinkAdminController(OttWatchLinkService ottWatchLinkService) {
        this.ottWatchLinkService = ottWatchLinkService;
        this.projectRoot = Path.of("").toAbsolutePath().normalize();
    }

    @GetMapping(value = "/candidates.csv", produces = "text/csv;charset=UTF-8")
    public ResponseEntity<String> exportCandidatesCsv(@RequestParam(defaultValue = "200") int limit) {
        List<OttWatchLinkService.CrawlCandidate> candidates = ottWatchLinkService.findCrawlCandidates(limit);
        StringBuilder csv = new StringBuilder();
        csv.append("movie_id,movie_cd,title,release_date,poster_image_url,kinolights_url,priority_reason\n");
        for (OttWatchLinkService.CrawlCandidate candidate : candidates) {
            csv.append(toCsvLine(candidate.toCsvRow())).append('\n');
        }

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=kinolights_candidates.csv")
                .contentType(new MediaType("text", "csv", StandardCharsets.UTF_8))
                .body(csv.toString());
    }

    @PostMapping("/import-csv")
    public OttWatchLinkService.ImportResult importCrawlerCsv(@RequestParam String path) {
        Path csvPath = resolveProjectPath(path);
        if (!Files.exists(csvPath)) {
            throw new ResponseStatusException(BAD_REQUEST, "CSV file not found: " + path);
        }

        try {
            List<Map<String, String>> csvRows = readCsv(csvPath);
            List<OttWatchLinkService.CrawlerRow> rows = csvRows.stream()
                    .map(this::toCrawlerRow)
                    .toList();
            return ottWatchLinkService.importCrawlerRows(rows);
        } catch (IOException e) {
            throw new ResponseStatusException(BAD_REQUEST, "CSV read failed: " + e.getMessage(), e);
        }
    }

    private Path resolveProjectPath(String rawPath) {
        Path resolved = projectRoot.resolve(rawPath).normalize();
        if (!resolved.startsWith(projectRoot)) {
            throw new ResponseStatusException(BAD_REQUEST, "Path must stay inside project root.");
        }
        return resolved;
    }

    private OttWatchLinkService.CrawlerRow toCrawlerRow(Map<String, String> row) {
        return new OttWatchLinkService.CrawlerRow(
                parseLong(row.get("movie_id")),
                row.getOrDefault("movie_cd", ""),
                row.getOrDefault("title", ""),
                row.getOrDefault("kinolights_url", ""),
                row.getOrDefault("status", ""),
                row.getOrDefault("provider", ""),
                row.getOrDefault("watch_url", ""),
                row.getOrDefault("raw_url", ""),
                row.getOrDefault("raw_text", ""),
                row.getOrDefault("source_method", ""),
                Boolean.parseBoolean(row.getOrDefault("is_external_direct", "false")),
                row.getOrDefault("error", ""),
                parseInstant(row.get("crawled_at"))
        );
    }

    private Long parseLong(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private Instant parseInstant(String value) {
        return OttWatchLinkService.parseInstant(value);
    }

    private String toCsvLine(List<String> values) {
        return values.stream()
                .map(OttWatchLinkService::escapeCsv)
                .reduce((left, right) -> left + "," + right)
                .orElse("");
    }

    private List<Map<String, String>> readCsv(Path path) throws IOException {
        String content = Files.readString(path, StandardCharsets.UTF_8);
        List<List<String>> rows = parseCsv(content);
        if (rows.isEmpty()) {
            return List.of();
        }

        List<String> headers = new ArrayList<>(rows.getFirst());
        if (!headers.isEmpty()) {
            headers.set(0, stripBom(headers.getFirst()));
        }
        List<Map<String, String>> result = new ArrayList<>();
        for (int i = 1; i < rows.size(); i++) {
            List<String> row = rows.get(i);
            if (row.stream().allMatch(String::isBlank)) {
                continue;
            }
            Map<String, String> mapped = new LinkedHashMap<>();
            for (int col = 0; col < headers.size(); col++) {
                mapped.put(headers.get(col), col < row.size() ? row.get(col) : "");
            }
            result.add(mapped);
        }
        return result;
    }

    private List<List<String>> parseCsv(String content) {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean quoted = false;

        for (int i = 0; i < content.length(); i++) {
            char ch = content.charAt(i);
            if (quoted) {
                if (ch == '"') {
                    if (i + 1 < content.length() && content.charAt(i + 1) == '"') {
                        cell.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    cell.append(ch);
                }
                continue;
            }

            if (ch == '"') {
                quoted = true;
            } else if (ch == ',') {
                row.add(cell.toString());
                cell.setLength(0);
            } else if (ch == '\n') {
                row.add(stripCarriageReturn(cell.toString()));
                rows.add(row);
                row = new ArrayList<>();
                cell.setLength(0);
            } else {
                cell.append(ch);
            }
        }

        row.add(stripCarriageReturn(cell.toString()));
        if (row.size() > 1 || !row.getFirst().isBlank()) {
            rows.add(row);
        }
        return rows;
    }

    private String stripCarriageReturn(String value) {
        return value.endsWith("\r") ? value.substring(0, value.length() - 1) : value;
    }

    private String stripBom(String value) {
        return value != null && value.startsWith("﻿") ? value.substring(1) : value;
    }
}
