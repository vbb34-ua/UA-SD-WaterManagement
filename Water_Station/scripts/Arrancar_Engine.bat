@echo off
chcp 65001 > nul
REM Se trabaja desde la carpeta Water_Station (la que contiene bin\ y lib\)
cd /d "%~dp0.."
REM Arranca el Engine de una estacion (bin\WM_WS_E.jar ya compilado, no se recompila nada).
REM Uso: Arrancar_Engine.bat [broker_kafka] [puerto_monitor] [id_WS]

call :buscar_java || exit /b 1

set KAFKA_BROKER=%1
set MONITOR_PORT=%2
set ID_WS=%3

REM Por defecto: TCP Proxy de Kafka en Railway (-> :9092)
if "%KAFKA_BROKER%"=="" set KAFKA_BROKER=iriguchi.proxy.rlwy.net:38689
if "%ID_WS%"=="" set /p ID_WS="ID de la estacion (el mismo que en su Monitor) [WS_01]: "
if "%ID_WS%"=="" set ID_WS=WS_01
if "%MONITOR_PORT%"=="" set /p MONITOR_PORT="Puerto local de su Monitor [7000]: "
if "%MONITOR_PORT%"=="" set MONITOR_PORT=7000

echo.
echo ==============================================================
echo   ENGINE (WM_WS_E) - %ID_WS%
echo   Kafka: %KAFKA_BROKER%  ^|  Monitor: localhost:%MONITOR_PORT%
echo ==============================================================
java -jar bin\WM_WS_E.jar %KAFKA_BROKER% localhost %MONITOR_PORT% %ID_WS%
pause
exit /b 0

:buscar_java
where java >nul 2>nul && exit /b 0
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" (
    set "PATH=%JAVA_HOME%\bin;%PATH%"
    exit /b 0
)
for %%D in ("%ProgramFiles%\Eclipse Adoptium" "%ProgramFiles%\Java" "%ProgramFiles%\Microsoft" "%LocalAppData%\Programs\Eclipse Adoptium") do (
    if exist %%D for /f "delims=" %%J in ('dir /b /ad /o-n %%D 2^>nul') do (
        if exist "%%~D\%%J\bin\java.exe" (
            set "PATH=%%~D\%%J\bin;%PATH%"
            exit /b 0
        )
    )
)
echo ERROR: no se encuentra Java (11 o superior). Instalalo desde https://adoptium.net/
pause
exit /b 1
