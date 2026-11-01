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
echo   COMPILANDO OPERARIO (MODO PORTABLE)...
echo ==============================================================
javac -encoding UTF-8 -cp ".;lib\*" WM_FO.java
if errorlevel 1 (
    echo Error al compilar WM_FO.java
    pause
    exit /b %errorlevel%
)

REM --- CONFIGURACION DE CONEXION ---
set KAFKA_BROKER=%1
set OP_ID=%2
set CENTRAL_HOST=%3
set CENTRAL_PORT=%4

if "%KAFKA_BROKER%"=="" set KAFKA_BROKER=iriguchi.proxy.rlwy.net:38689
if "%OP_ID%"=="" set OP_ID=FO_01
if "%CENTRAL_HOST%"=="" set /p CENTRAL_HOST="Introduce el HOST del TCP Proxy de Central (ej. proxy.rlwy.net): "
if "%CENTRAL_PORT%"=="" set /p CENTRAL_PORT="Introduce el PUERTO del TCP Proxy de Central (ej. 12345): "

echo.
echo ==============================================================
echo   ARRANCANDO FIELD OPERATOR (WM_FO) - %OP_ID%
echo ==============================================================
echo Broker Kafka:        %KAFKA_BROKER%
echo Central:             %CENTRAL_HOST%:%CENTRAL_PORT%
echo ==============================================================

java -cp ".;lib\*" WM_FO %KAFKA_BROKER% %OP_ID% %CENTRAL_HOST% %CENTRAL_PORT%
pause