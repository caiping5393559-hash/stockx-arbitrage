param()

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$baseDir = Split-Path -Parent $scriptDir
$python = Join-Path $baseDir ".venv\Scripts\python.exe"
$app = Join-Path $baseDir "app.py"
$dataDir = Join-Path $baseDir "data"
$desktopShortcut = Join-Path ([Environment]::GetFolderPath("Desktop")) "StockX_GOAT_Starter.lnk"
$stdoutLog = Join-Path $dataDir "streamlit_start_stdout.log"
$stderrLog = Join-Path $dataDir "streamlit_start_stderr.log"

if (!(Test-Path $python)) {
    Write-Host "Python not found at $python"
    exit 1
}

if (!(Test-Path $dataDir)) {
    New-Item -ItemType Directory -Path $dataDir | Out-Null
}

# Do not auto-start heavy API workers on app launch. The UI should stay light;
# API jobs are started explicitly from the relevant page.
function Stop-ProjectPythonProcesses {
    try {
        $escapedBase = [WildcardPattern]::Escape($baseDir)
        $processes = Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" -ErrorAction Stop |
            Where-Object {
                $_.CommandLine -and
                $_.CommandLine -like "*$baseDir*" -and
                $_.ProcessId -ne $PID
            }
        foreach ($proc in $processes) {
            try {
                Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
                Write-Host "Stopped old project process PID $($proc.ProcessId)"
            } catch {}
        }
    } catch {
        Write-Host "Process cleanup skipped: $($_.Exception.Message)"
    }
}

function Clear-ProjectLocks {
    $locks = @(
        (Join-Path $dataDir "goat_stockx_worker.lock")
    )
    foreach ($lock in $locks) {
        try {
            if ((Test-Path $lock) -and ((Resolve-Path $lock).Path.StartsWith((Resolve-Path $dataDir).Path))) {
                Remove-Item -LiteralPath $lock -Force -ErrorAction SilentlyContinue
            }
        } catch {}
    }
}

function Wait-StreamlitHealth {
    param([int]$Seconds = 30)
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $health = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:8501/_stcore/health" -TimeoutSec 3
            if ($health.Content -match "ok") {
                return $true
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    return $false
}

try {
    $wsh = New-Object -ComObject WScript.Shell
    $shortcut = $wsh.CreateShortcut($desktopShortcut)
    $shortcut.TargetPath = "$env:WINDIR\System32\cmd.exe"
    $shortcut.Arguments = "/c `"$baseDir\run_streamlit.cmd`""
    $shortcut.WorkingDirectory = $baseDir
    $shortcut.Description = "Launch StockX / GOAT arbitrage scanner"
    $shortcut.IconLocation = "$env:WINDIR\System32\shell32.dll,167"
    $shortcut.Save()
} catch {
    Write-Host "Desktop shortcut could not be created: $($_.Exception.Message)"
}

Stop-ProjectPythonProcesses
Clear-ProjectLocks

Write-Host "Starting Streamlit on http://localhost:8501 ..."
$args = @(
    "-m", "streamlit", "run", $app,
    "--server.headless", "true",
    "--server.port", "8501",
    "--browser.gatherUsageStats", "false",
    "--server.folderWatchBlacklist", "data"
)
Start-Process -FilePath $python -ArgumentList $args -WorkingDirectory $baseDir -WindowStyle Hidden -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog | Out-Null

if (Wait-StreamlitHealth -Seconds 35) {
    Write-Host "Opening http://localhost:8501 ..."
    Start-Process "http://localhost:8501/" | Out-Null
} else {
    Write-Host "Streamlit did not become ready in time. Check logs:"
    Write-Host $stdoutLog
    Write-Host $stderrLog
}
