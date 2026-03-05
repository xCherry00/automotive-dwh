param(
    [Parameter(Mandatory = $true)]
    [string]$Date,

    [ValidateSet('dev','large','xlarge')]
    [string]$Profile = 'dev',

    [switch]$InitDb,
    [switch]$SkipGenerate,
    [switch]$SkipDq,

    [string]$PythonExe = 'python'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    Write-Host "`n=== $Name ===" -ForegroundColor Cyan
    & $Action
}

try {
    # Moment 0: ustawienie katalogu projektu dla kolejnych komend.
    $projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
    Set-Location $projectRoot

    if (-not $SkipGenerate) {
        # Moment 1: generowanie paczki plikow wejsciowych CSV.
        Invoke-Step -Name 'Generate data' -Action {
            & $PythonExe .\generate_data.py --date $Date --profile $Profile --output-root data/incoming
        }
    }

    # Moment 2: wykonanie pipeline ETL (opcjonalnie z inicjalizacja DDL).
    Invoke-Step -Name 'Run pipeline' -Action {
        if ($InitDb) {
            & $PythonExe .\main.py --date $Date --init-db
        }
        else {
            & $PythonExe .\main.py --date $Date
        }
    }

    if (-not $SkipDq) {
        # Moment 3: kontrola jakosci po zaladowaniu danych.
        Invoke-Step -Name 'Run DQ checks' -Action {
            Get-Content .\tests\dq_checks.sql -Raw | docker exec -i automotive_dwh_db psql -U automotive -d automotive_dwh
        }
    }

    Write-Host "`nDone. Batch date: $Date" -ForegroundColor Green
}
catch {
    Write-Error $_
    exit 1
}
