@echo off
chcp 65001 > nul
REM Se trabaja desde la carpeta Water_Station (la que contiene bin\ y lib\)
cd /d "%~dp0.."
REM Arranca la aplicacion del operario (bin\WM_FO.jar ya compilado, no se recompila nada).
REM Uso: Arrancar_Operario.bat [broker_kafka] [id_operario] [host_central] [puerto_central] [fichero_servicios]

call :buscar_java || exit /b 1

set KAFKA_BROKER=%1
set OP_ID=%2
set CENTRAL_HOST=%3
set CENTRAL_PORT=%4
set FICHERO=%~5

REM Por defecto: TCP Proxy de Kafka y de WM_Central en Railway
if "%KAFKA_BROKER%"=="" set KAFKA_BROKER=iriguchi.proxy.rlwy.net:38689
if "%CENTRAL_HOST%"=="" set CENTRAL_HOST=monorail.proxy.rlwy.net
if "%CENTRAL_PORT%"=="" set CENTRAL_PORT=32265
if "%OP_ID%"=="" set /p OP_ID="ID del operario a registrar [FO_01]: "
if "%OP_ID%"=="" set OP_ID=FO_01
if "%FICHERO%"=="" set /p FICHERO="Fichero de servicios para modo automatico, p. ej. servicios_ejemplo.txt (Enter = solo menu manual): "
REM Si solo se da el nombre, se busca en la carpeta servicios\
if not "%FICHERO%"=="" if not exist "%FICHERO%" if exist "servicios\%FICHERO%" set "FICHERO=servicios\%FICHERO%"

echo.
echo ==============================================================
echo   FIELD OPERATOR (WM_FO) - %OP_ID%
echo   Central: %CENTRAL_HOST%:%CENTRAL_PORT%  ^|  Kafka: %KAFKA_BROKER%
if not "%FICHERO%"=="" echo   Servicios automaticos: %FICHERO%
echo ==============================================================
java -jar bin\WM_FO.jar %KAFKA_BROKER% %OP_ID% %CENTRAL_HOST% %CENTRAL_PORT% "%FICHERO%"
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
