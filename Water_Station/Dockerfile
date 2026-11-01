# Imagen única para los módulos de laboratorio: WM_WS_M (Monitor), WM_WS_E (Engine) y WM_FO (Operario).
# El módulo a ejecutar se elige con el comando del contenedor (ver compose.yml o README).
#
#   docker build -t wm-lab ./Water_Station
#   docker run -it --rm wm-lab WM_FO <broker> FO_01 <host_central> <puerto_central>

FROM eclipse-temurin:21-jdk AS build
WORKDIR /build
COPY lib ./lib
COPY WM_WS_M.java WM_WS_E.java WM_FO.java ./
RUN javac -encoding UTF-8 -cp "lib/*" -d out *.java

FROM eclipse-temurin:21-jre
LABEL description="WaterManagement - Watering Station (Monitor + Engine) y Field Operator"
WORKDIR /app
COPY --from=build /build/out ./
COPY lib ./lib
# Ficheros de servicios del operario (montar un volumen aquí para usar los vuestros)
COPY servicios_ejemplo.txt ./servicios/servicios_ejemplo.txt

ENTRYPOINT ["java", "-cp", ".:lib/*"]
CMD ["WM_FO"]
