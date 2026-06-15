$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $Root "data"
$ErrorLog = Join-Path $LogDir "start_frontend_error.txt"
$RunLog = Join-Path $LogDir "streamlit_frontend.log"

try {
  New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
  Set-Location -LiteralPath $Root

  $Streamlit = Join-Path $Root ".venv\Scripts\streamlit.exe"
  $App = Join-Path $Root "app.py"

  & $Streamlit run $App `
    --server.headless true `
    --server.port 8501 `
    --browser.gatherUsageStats false `
    --server.folderWatchBlacklist data *> $RunLog
} catch {
  $Message = "$(Get-Date -Format s) $($_.Exception.ToString())"
  Set-Content -LiteralPath $ErrorLog -Value $Message -Encoding UTF8
  throw
}
