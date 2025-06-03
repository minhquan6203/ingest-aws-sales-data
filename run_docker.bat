@echo off
echo Building and starting Docker containers for AWS Sales Data ETL...

REM Build and start the containers
docker-compose up --build

echo Done!
pause 