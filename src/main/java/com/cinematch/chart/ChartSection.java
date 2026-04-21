package com.cinematch.chart;

import java.util.List;

public record ChartSection(ChartEntry entry, List<ChartMovieRow> rows) {}