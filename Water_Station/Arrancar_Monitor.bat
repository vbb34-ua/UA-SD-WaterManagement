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
echo   COMPILANDO MONITOR (MODO PORTABLE)...
echo ==============================================================
javac -encoding UTF-8 WM_WS_M.java
if errorlevel 1 (
    echo Error al compilar WM_WS_M.java
    pause
    exit /b %errorlevel%
)

REM --- CONFIGURACION DE CONEXION A CENTRAL ---
set CENTRAL_HOST=%1
set CENTRAL_PORT=%2
set WS_ID=%3
set UBICACION=%~4
set MONITOR_PORT=%5

REM Por defecto: TCP Proxy de WM_Central en Railway (-> :6000)
if "%CENTRAL_HOST%"=="" set CENTRAL_HOST=monorail.proxy.rlwy.net
if "%CENTRAL_PORT%"=="" set CENTRAL_PORT=32265
if "%WS_ID%"=="" set /p WS_ID="ID de la estacion a registrar [WS_01]: "
if "%WS_ID%"=="" set WS_ID=WS_01
if "%UBICACION%"=="" set /p UBICACION="Ubicacion de la estacion [Parque Central]: "
if "%UBICACION%"=="" set UBICACION=Parque Central
if "%MONITOR_PORT%"=="" set /p MONITOR_PORT="Puerto local para el Engine (distinto por cada WS en el mismo PC) [7000]: "
if "%MONITOR_PORT%"=="" set MONITOR_PORT=7000

echo.
echo ==============================================================
echo   ARRANCANDO MONITOR (WM_WS_M) - %WS_ID%
echo ==============================================================
echo Conectando a Central: %CENTRAL_HOST%:%CENTRAL_PORT%
echo Ubicacion:            %UBICACION%
echo Escuchando a Engine en puerto local: %MONITOR_PORT%
echo ==============================================================

java -cp . WM_WS_M %MONITOR_PORT% %CENTRAL_HOST% %CENTRAL_PORT% %WS_ID% "%UBICACION%"
pause