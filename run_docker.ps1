# PowerShell script to check if Docker is running and start the containers

# Function to check if Docker is running
function Test-DockerRunning {
    try {
        $result = docker info 2>&1
        if ($LASTEXITCODE -eq 0) {
            return $true
        } else {
            return $false
        }
    } catch {
        return $false
    }
}

# Check if Docker is running
if (-not (Test-DockerRunning)) {
    Write-Host "Docker is not running. Please start Docker Desktop and try again." -ForegroundColor Red
    Write-Host "Press any key to exit..."
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
    exit 1
}

# Create required directories
if (-not (Test-Path -Path ".\logs")) {
    New-Item -Path ".\logs" -ItemType Directory | Out-Null
    Write-Host "Created logs directory" -ForegroundColor Green
}

if (-not (Test-Path -Path ".\data")) {
    New-Item -Path ".\data" -ItemType Directory | Out-Null
    Write-Host "Created data directory" -ForegroundColor Green
}

# Build and start the containers
Write-Host "Building and starting Docker containers for AWS Sales Data ETL..." -ForegroundColor Cyan
docker-compose down
docker-compose up --build

Write-Host "Done!" -ForegroundColor Green
Write-Host "Press any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown") 