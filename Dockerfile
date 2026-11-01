# Imagen de WM_Central (la que despliega Railway). Se construye en dos fases:
# 1) compila con Maven un JAR con todas las dependencias, 2) lo ejecuta sobre un JRE ligero.

FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# Primero solo el pom.xml: si no cambia, Docker reutiliza las dependencias ya descargadas
COPY CoreSystem/pom.xml ./

# Si esta descarga previa falla, el build no se detiene: Maven las vuelve a pedir en el package
RUN mvn dependency:resolve -B --no-transfer-progress || true

COPY CoreSystem/src ./src

RUN mvn clean package -B --no-transfer-progress -DskipTests


FROM eclipse-temurin:21-jre

LABEL maintainer="WaterManagement Team"
LABEL description="CENTRAL - Sistema de gestión de riego WaterManagement"

WORKDIR /app

# Carpeta de la BD SQLite: en Railway se monta aquí un volumen para que no se pierda al redesplegar
RUN mkdir -p /app/data

COPY --from=build /build/target/central-1.0-SNAPSHOT.jar app.jar

# 8080: panel web (HTTP)  |  6000: sockets de estaciones y operarios
EXPOSE 8080
EXPOSE 6000

# Valores por defecto; en Railway se sobrescriben con las variables del servicio
ENV PORT=8080
ENV SOCKET_PORT=6000
ENV KAFKA_BROKER=localhost:9092
ENV DB_PATH=/app/data/water_management.db

CMD ["sh", "-c", "java -jar app.jar ${SOCKET_PORT} ${KAFKA_BROKER}"]
