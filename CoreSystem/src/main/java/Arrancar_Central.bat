@echo off
chcp 65001 > nul
cd /d "%~dp0"
REM Arranque LOCAL de la Central (para pruebas). En la evaluación se despliega en Railway con Docker.
REM Uso: Arrancar_Central.bat [puerto_sockets] [broker_kafka]

set SOCKET_PORT=%1
set KAFKA_BROKER=%2
if "%SOCKET_PORT%"=="" set SOCKET_PORT=6000
if "%KAFKA_BROKER%"=="" set KAFKA_BROKER=localhost:9092

REM kafka-clients se toma de Water_Station\lib; index.html de src\main\resources
set CP=.;..\resources;sqlite-jdbc.jar;slf4j-api.jar;slf4j-simple.jar;..\..\..\..\Water_Station\lib\kafka-clients-4.4.0.jar

echo ==============================================
echo   COMPILANDO EL SERVIDOR CENTRAL...
echo ==============================================
javac -encoding UTF-8 -cp "%CP%" *.java
if errorlevel 1 (
    echo Error al compilar la Central
    pause
    exit /b %errorlevel%
)

echo.
echo ==============================================
echo   ARRANCANDO WATER MANAGEMENT SYSTEM...
echo   Sockets: %SOCKET_PORT%  ^|  Kafka: %KAFKA_BROKER%  ^|  Web: http://localhost:8080
echo ==============================================
java -cp "%CP%" WM_Central %SOCKET_PORT% %KAFKA_BROKER%
pause
