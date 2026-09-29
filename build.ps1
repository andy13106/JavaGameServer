param([switch]$SkipTests)
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    if ($SkipTests) { & mvn -B -ntp '-DskipTests' package }
    else { & mvn -B -ntp verify }
    if ($LASTEXITCODE -ne 0) { throw "Maven build failed ($LASTEXITCODE)" }
} finally { Pop-Location }
