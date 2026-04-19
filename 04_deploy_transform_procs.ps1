<#
Deploy all SQL transform procs to the target SQL Server using `sqlcmd`.
Usage examples:
  # Windows integrated auth
  .\04_deploy_transform_procs.ps1 -ServerInstance "localhost\\SQLEXPRESS"
  .\04_deploy_transform_procs.ps1 -ServerInstance "."

  # SQL auth
  .\04_deploy_transform_procs.ps1 -ServerInstance "myserver" -Username "sa" -Password "Secret123"
#>
param(
    [Parameter(Mandatory=$true)]
    [string]$ServerInstance,

    [string]$Username,
    [string]$Password,

    [string]$Database = 'DW_Synthea_Staging',

    [switch]$TrustServerCertificate
)

$scriptFolder = Join-Path $PSScriptRoot 'transform_procs'
if (-not (Test-Path $scriptFolder)) {
    Write-Error "transform_procs folder not found at $scriptFolder"
    exit 2
}

# find sql files (ordered)
$sqlFiles = Get-ChildItem -Path $scriptFolder -Filter "usp_transform_landing_to_staging_*.sql" | Sort-Object Name
if ($sqlFiles.Count -eq 0) {
    Write-Error "No transform proc files found in $scriptFolder"
    exit 3
}

$errors = @()

foreach ($file in $sqlFiles) {
    Write-Host "Deploying $($file.Name)..." -ForegroundColor Cyan
    $filePath = $file.FullName
    $args = @('-S', $ServerInstance, '-d', $Database, '-i', $filePath, '-b')

    if ($PSVersionTable.PSVersion.Major -ge 6) {
        # On PS Core / non-Windows, ensure sqlcmd is on PATH
    }

    if ([string]::IsNullOrEmpty($Username)) {
        $args += '-E'
    } else {
        $args += @('-U', $Username, '-P', $Password)
    }

    if ($TrustServerCertificate.IsPresent) {
        $args += '-C'
    }

    $proc = Start-Process -FilePath 'sqlcmd' -ArgumentList $args -NoNewWindow -Wait -PassThru -ErrorAction SilentlyContinue
    if ($proc -eq $null) {
        Write-Error "Failed to start sqlcmd. Ensure the SQL Server command-line tools are installed and `sqlcmd` is on PATH."
        $errors += $file.Name
        break
    }

    if ($proc.ExitCode -ne 0) {
        Write-Error "sqlcmd returned exit code $($proc.ExitCode) for $($file.Name)"
        $errors += $file.Name
        break
    }
    Write-Host "Deployed $($file.Name) OK" -ForegroundColor Green
}

if ($errors.Count -gt 0) {
    Write-Error "Deployment finished with errors: $($errors -join ', ')"
    exit 1
} else {
    Write-Host "All transform procs deployed successfully." -ForegroundColor Green
    exit 0
}
