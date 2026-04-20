package movie.web.login.chart;

import java.util.List;

public record ChartSection(ChartEntry entry, List<ChartMovieRow> rows) {}
