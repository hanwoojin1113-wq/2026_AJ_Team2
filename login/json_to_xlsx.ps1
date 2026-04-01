param(
    [Parameter(Mandatory = $true)]
    [string]$SourcePath,
    [string]$OutputPath = '.\2025-50.xlsx'
)

$data = Get-Content -LiteralPath $SourcePath -Raw -Encoding UTF8 | ConvertFrom-Json
$movies = $data.movieListResult.movieList

function Escape-Xml([string]$text) {
    if ($null -eq $text) { return '' }
    return [System.Security.SecurityElement]::Escape([string]$text)
}

function ColName([int]$n) {
    $name = ''
    while ($n -gt 0) {
        $rem = ($n - 1) % 26
        $name = [char](65 + $rem) + $name
        $n = [math]::Floor(($n - 1) / 26)
    }
    return $name
}

function Write-Utf8File([string]$path, [string]$content) {
    $dir = Split-Path -Parent $path
    if ($dir -and -not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    [System.IO.File]::WriteAllText($path, $content, [System.Text.UTF8Encoding]::new($false))
}

$headers = @(
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
    'companys'
)

$rows = New-Object System.Collections.Generic.List[object[]]
foreach ($m in $movies) {
    $directorNames = if ($m.directors) { ($m.directors | ForEach-Object { $_.peopleNm }) -join ', ' } else { '' }
    $companyNames = if ($m.companys) { ($m.companys | ForEach-Object { $_.companyNm }) -join ', ' } else { '' }
    $rows.Add(@(
        [string]$m.movieCd,
        [string]$m.movieNm,
        [string]$m.movieNmEn,
        [string]$m.prdtYear,
        [string]$m.openDt,
        [string]$m.typeNm,
        [string]$m.prdtStatNm,
        [string]$m.nationAlt,
        [string]$m.genreAlt,
        [string]$m.repNationNm,
        [string]$m.repGenreNm,
        [string]$directorNames,
        [string]$companyNames
    ))
}

$types = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
'@

$rels = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
'@

$app = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Microsoft Excel</Application>
</Properties>
'@

$now = (Get-Date).ToUniversalTime().ToString('s') + 'Z'
$core = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>Codex</dc:creator>
  <cp:lastModifiedBy>Codex</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">$now</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">$now</dcterms:modified>
</cp:coreProperties>
"@

$workbook = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="movieList" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
'@

$workbookRels = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
'@

$styles = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
  <cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>
'@

$sheet = New-Object System.Text.StringBuilder
[void]$sheet.AppendLine('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
[void]$sheet.AppendLine('<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">')
[void]$sheet.AppendLine('  <sheetData>')

$allRows = New-Object System.Collections.Generic.List[object[]]
$allRows.Add($headers)
foreach ($row in $rows) {
    $allRows.Add($row)
}

for ($r = 0; $r -lt $allRows.Count; $r++) {
    $rowNumber = $r + 1
    [void]$sheet.Append("    <row r=`"$rowNumber`">")
    $current = $allRows[$r]
    for ($c = 0; $c -lt $current.Length; $c++) {
        $cellRef = "$(ColName ($c + 1))$rowNumber"
        $value = Escape-Xml $current[$c]
        [void]$sheet.Append("<c r=`"$cellRef`" t=`"inlineStr`"><is><t>$value</t></is></c>")
    }
    [void]$sheet.AppendLine('</row>')
}

$lastCell = "$(ColName $headers.Count)$($allRows.Count)"
[void]$sheet.AppendLine('  </sheetData>')
[void]$sheet.AppendLine("  <dimension ref=`"A1:$lastCell`"/>")
[void]$sheet.AppendLine('</worksheet>')

if (Test-Path -LiteralPath $OutputPath) {
    Remove-Item -LiteralPath $OutputPath -Force
}

$tempRoot = Join-Path $env:TEMP ("xlsx_" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot -Force | Out-Null

Write-Utf8File (Join-Path $tempRoot '[Content_Types].xml') $types
Write-Utf8File (Join-Path $tempRoot '_rels\.rels') $rels
Write-Utf8File (Join-Path $tempRoot 'docProps\app.xml') $app
Write-Utf8File (Join-Path $tempRoot 'docProps\core.xml') $core
Write-Utf8File (Join-Path $tempRoot 'xl\workbook.xml') $workbook
Write-Utf8File (Join-Path $tempRoot 'xl\_rels\workbook.xml.rels') $workbookRels
Write-Utf8File (Join-Path $tempRoot 'xl\styles.xml') $styles
Write-Utf8File (Join-Path $tempRoot 'xl\worksheets\sheet1.xml') $sheet.ToString()

$zipPath = [System.IO.Path]::ChangeExtension($OutputPath, '.zip')
if (Test-Path -LiteralPath $zipPath) {
    Remove-Item -LiteralPath $zipPath -Force
}

Compress-Archive -Path (Join-Path $tempRoot '*') -DestinationPath $zipPath -Force
Move-Item -LiteralPath $zipPath -Destination $OutputPath -Force
Remove-Item -LiteralPath $tempRoot -Recurse -Force

Get-Item -LiteralPath $OutputPath | Format-List FullName,Length,LastWriteTime
