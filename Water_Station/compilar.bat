@echo off
chcp 65001 > nul
cd /d "%~dp0"
REM ============================================================================
REM  Genera WM_WS_M.jar, WM_WS_E.jar y WM_FO.jar a partir del código fuente.
REM  SOLO para desarrollo: en la corrección se usan los .jar ya generados
REM  (los Arrancar_*.bat no compilan nada). Requiere un JDK 11 o superior.
REM ============================================================================

where javac >nul 2>nul
if errorlevel 1 (
    echo ERROR: no se encuentra javac. Instala un JDK 11+ o configura JAVA_HOME.
    pause
    exit /b 1
)

if exist build rmdir /s /q build
mkdir build

echo Compilando fuentes (Java 11)...
javac --release 11 -encoding UTF-8 -cp "lib\*" -d build Protocolo.java WM_WS_M.java WM_WS_E.java WM_FO.java
if errorlevel 1 (
    echo Error de compilacion.
    pause
    exit /b 1
)

REM Manifest con las librerias de Kafka (ruta relativa al .jar)
echo Class-Path: lib/kafka-clients-4.4.0.jar lib/slf4j-api.jar> manifest.tmp

for %%M in (WM_WS_M WM_WS_E WM_FO) do (
    echo Generando %%M.jar...
    jar --create --file %%M.jar --manifest manifest.tmp --main-class %%M -C build .
)

del manifest.tmp
rmdir /s /q build
echo.
echo Listo: WM_WS_M.jar, WM_WS_E.jar y WM_FO.jar
pause
