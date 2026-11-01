# Imagen única para los módulos de laboratorio: WM_WS_M (Monitor), WM_WS_E (Engine) y WM_FO (Operario).
# Usa los .jar ya compilados de bin/ (no se compila nada al construir ni al arrancar).
# El módulo a ejecutar se elige con el comando del contenedor (ver compose.yml o README).
#
#   docker build -t wm-lab ./Water_Station
#   docker run -it --rm wm-lab WM_FO <broker> FO_01 <host_central> <puerto_central> [fichero]

FROM eclipse-temurin:21-jre
LABEL description="WaterManagement - Watering Station (Monitor + Engine) y Field Operator"
WORKDIR /app
COPY lib ./lib
COPY bin ./bin
# Ficheros de servicios del operario (montar un volumen en /app/servicios para usar otros)
COPY servicios ./servicios

# Los tres .jar contienen todas las clases: basta uno en el classpath
ENTRYPOINT ["java", "-cp", "bin/WM_FO.jar:lib/*"]
CMD ["WM_FO"]
