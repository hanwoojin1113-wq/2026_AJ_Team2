const fs = require('fs');
const path = require('path');

const [, , sourcePath, outputPath] = process.argv;

if (!sourcePath || !outputPath) {
  console.error('Usage: node json_to_csv.js <sourcePath> <outputPath>');
  process.exit(1);
}

const raw = fs.readFileSync(sourcePath, 'utf8');
const data = JSON.parse(raw);
const movies = data.movieListResult.movieList || [];

const headers = [
  'movieCd',
  'movieNm',
  'movieNmEn',
  'prdtYear',
  'openDt',
  'typeNm',
  'prdtStatNm',
  'nationAlt',
  'genreAlt',
  'repNationNm',
  'repGenreNm',
  'directors',
  'companys',
];

function csvEscape(value) {
  const text = String(value ?? '');
  return `"${text.replace(/"/g, '""')}"`;
}

const lines = [];
lines.push(headers.map(csvEscape).join(','));

for (const movie of movies) {
  const row = [
    movie.movieCd ?? '',
    movie.movieNm ?? '',
    movie.movieNmEn ?? '',
    movie.prdtYear ?? '',
    movie.openDt ?? '',
    movie.typeNm ?? '',
    movie.prdtStatNm ?? '',
    movie.nationAlt ?? '',
    movie.genreAlt ?? '',
    movie.repNationNm ?? '',
    movie.repGenreNm ?? '',
    (movie.directors || []).map((d) => d.peopleNm).join(', '),
    (movie.companys || []).map((c) => c.companyNm).join(', '),
  ];
  lines.push(row.map(csvEscape).join(','));
}

const csv = '\uFEFF' + lines.join('\r\n');
fs.writeFileSync(outputPath, csv, 'utf8');

const stat = fs.statSync(outputPath);
console.log(JSON.stringify({
  fullName: path.resolve(outputPath),
  length: stat.size,
}));
