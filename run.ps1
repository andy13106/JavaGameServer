param([switch]$Serve)
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    $jarPath = Join-Path $PSScriptRoot 'game-demo/target/game-demo-0.1.0-SNAPSHOT.jar'
    if (!(Test-Path -LiteralPath $jarPath)) { throw 'Run ./build.ps1 first.' }
    if ($Serve) { & java -jar $jarPath --serve } else { & java -jar $jarPath }
    if ($LASTEXITCODE -ne 0) { throw "Demo failed ($LASTEXITCODE)" }
} finally { Pop-Location }
