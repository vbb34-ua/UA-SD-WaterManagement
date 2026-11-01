@echo off
chcp 65001 > nul
REM Se trabaja desde la carpeta Water_Station (src\ -> bin\, usando lib\)
cd /d "%~dp0.."
REM ============================================================================
REM  Genera bin\WM_WS_M.jar, bin\WM_WS_E.jar y bin\WM_FO.jar a partir de src\.
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
javac --release 11 -encoding UTF-8 -cp "lib\*" -d build src\Protocolo.java src\WM_WS_M.java src\WM_WS_E.java src\WM_FO.java
if errorlevel 1 (
    echo Error de compilacion.
    pause
    exit /b 1
)

REM Manifest con las librerias de Kafka (ruta relativa al .jar: bin\ -> ..\lib\)
REM slf4j-nop descarta los logs internos de Kafka (evita el aviso "No SLF4J providers were found")
echo Class-Path: ../lib/kafka-clients-4.4.0.jar ../lib/slf4j-api.jar ../lib/slf4j-nop-2.0.12.jar> manifest.tmp

if not exist bin mkdir bin
for %%M in (WM_WS_M WM_WS_E WM_FO) do (
    echo Generando bin\%%M.jar...
    jar --create --file bin\%%M.jar --manifest manifest.tmp --main-class %%M -C build .
)

del manifest.tmp
rmdir /s /q build
echo.
echo Listo: bin\WM_WS_M.jar, bin\WM_WS_E.jar y bin\WM_FO.jar
pause
