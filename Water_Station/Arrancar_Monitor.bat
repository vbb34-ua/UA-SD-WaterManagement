@echo off
chcp 65001 > nul
cd /d "%~dp0"
REM Arranca el Monitor de una estacion (WM_WS_M.jar ya compilado, no se recompila nada).
REM Uso: Arrancar_Monitor.bat [host_central] [puerto_central] [id_WS] [ubicacion] [puerto_local_engine]

call :buscar_java || exit /b 1

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
if "%UBICACION%"=="" set /p UBICACION="Ubicacion (Enter = la registrada en CENTRAL): "
if "%MONITOR_PORT%"=="" set /p MONITOR_PORT="Puerto local para el Engine (distinto por cada WS en el mismo PC) [7000]: "
if "%MONITOR_PORT%"=="" set MONITOR_PORT=7000

echo.
echo ==============================================================
echo   MONITOR (WM_WS_M) - %WS_ID%
echo   Central: %CENTRAL_HOST%:%CENTRAL_PORT%  ^|  Puerto Engine: %MONITOR_PORT%
echo ==============================================================
java -jar WM_WS_M.jar %MONITOR_PORT% %CENTRAL_HOST% %CENTRAL_PORT% %WS_ID% "%UBICACION%"
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
