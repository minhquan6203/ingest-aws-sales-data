@echo off
echo Checking Docker status...

REM Check if Docker is running
docker info > nul 2>&1
if %errorlevel% neq 0 (
    echo Docker is not running. Please start Docker Desktop and try again.
    pause
    exit /b 1
)

REM Create required directories
if not exist ".\logs" (
    mkdir ".\logs"
    echo Created logs directory
)

if not exist ".\data" (
    mkdir ".\data"
    echo Created data directory
)

echo Building and starting Docker containers for AWS Sales Data ETL...

REM Stop any existing containers first
docker-compose down

REM Build and start the containers
docker-compose up --build

echo Done!
pause 