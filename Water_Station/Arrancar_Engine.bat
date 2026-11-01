@echo off
chcp 65001 > nul
cd /d "%~dp0"

REM --- AUTO-DETECCION DE JAVA ---
where javac >nul 2>nul
if %errorlevel% equ 0 goto :java_ok

if defined JAVA_HOME (
    set "PATH=%JAVA_HOME%\bin;%PATH%"
    where javac >nul 2>nul
    if %errorlevel% equ 0 goto :java_ok
)

REM Buscar en rutas tipicas de instalacion de Windows
for %%D in (
    "%ProgramFiles%\Eclipse Adoptium"
    "%ProgramFiles%\Java"
    "%ProgramFiles%\Microsoft"
    "%ProgramFiles(x86)%\Java"
    "%LocalAppData%\Programs\Eclipse Adoptium"
) do (
    if exist %%D (
        for /f "delims=" %%J in ('dir /b /ad /o-n %%D 2^>nul') do (
            if exist "%%~D\%%J\bin\javac.exe" (
                set "JAVA_HOME=%%~D\%%J"
                set "PATH=%%~D\%%J\bin;%PATH%"
                echo [INFO] Java encontrado en: %%~D\%%J
                goto :java_ok
            )
        )
    )
)

echo ==============================================================
echo   ERROR: No se encontro javac (compilador de Java).
echo   Instala un JDK (ej. Eclipse Temurin) desde:
echo   https://adoptium.net/
echo   O configura la variable JAVA_HOME.
echo ==============================================================
pause
exit /b 1

:java_ok
echo ==============================================================
echo   COMPILANDO ENGINE (MODO PORTABLE)...
echo ==============================================================
javac -encoding UTF-8 -cp ".;lib\*" WM_WS_E.java
if errorlevel 1 (
    echo Error al compilar WM_WS_E.java
    pause
    exit /b %errorlevel%
)

REM --- CONFIGURACION KAFKA Y MONITOR LOCAL ---
set KAFKA_BROKER=%1
set MONITOR_PORT=%2
set ID_WS=%3

if "%KAFKA_BROKER%"=="" set KAFKA_BROKER=iriguchi.proxy.rlwy.net:38689
if "%MONITOR_PORT%"=="" set MONITOR_PORT=7000
if "%ID_WS%"=="" set ID_WS=WS_01

echo.
echo ==============================================================
echo   ARRANCANDO ENGINE (WM_WS_E) - %ID_WS%
echo ==============================================================
echo Broker Kafka:        %KAFKA_BROKER%
echo Monitor Local:       localhost:%MONITOR_PORT%
echo ==============================================================

java -cp ".;lib\*" WM_WS_E %KAFKA_BROKER% localhost %MONITOR_PORT% %ID_WS%
pause