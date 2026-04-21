package com.cinematch.kobis;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KobisController {

    private final KobisMovieImportService kobisMovieImportService;
    private final KobisMovieNormalizeService kobisMovieNormalizeService;

    public KobisController(
            KobisMovieImportService kobisMovieImportService,
            KobisMovieNormalizeService kobisMovieNormalizeService
    ) {
        this.kobisMovieImportService = kobisMovieImportService;
        this.kobisMovieNormalizeService = kobisMovieNormalizeService;
    }

    @GetMapping("/kobis/test")
    public String testKobis() {
        return "KOBIS import endpoint is ready. Send POST /kobis/import-boxoffice, /kobis/import-yearly-representatives, /kobis/normalize";
    }

    @PostMapping("/kobis/import-boxoffice")
    public KobisMovieImportService.ImportResult importBoxOfficeMovies(
            @RequestParam(defaultValue = "300") int targetCount
    ) {
        return kobisMovieImportService.importWeeklyBoxOfficeMovies(targetCount);
    }

    @PostMapping("/kobis/import-yearly-representatives")
    public KobisMovieImportService.YearlyImportResult importYearlyRepresentativeMovies(
            @RequestParam(required = false) Integer startYear,
            @RequestParam(required = false) Integer endYear,
            @RequestParam(defaultValue = "30") int targetPerYear
    ) {
        return kobisMovieImportService.importYearlyRepresentativeMovies(startYear, endYear, targetPerYear);
    }

    @PostMapping("/kobis/normalize")
    public KobisMovieNormalizeService.NormalizeResult normalizeMovies() {
        return kobisMovieNormalizeService.normalizeRawMovies();
    }
}
