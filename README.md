# WaterManagement — Sistemas Distribuidos 26/27 (Release 1)

Simulación de la gestión centralizada del riego de los parques y jardines de una ciudad.
Componentes comunicados mediante **sockets** (protocolo `<STX><DATA><ETX><LRC>`) y **Apache Kafka**.

```
 PC LAB 1                     RAILWAY (Cloud)                      PC LAB 2
┌──────────┐  sockets  ┌──────────────────────────┐  sockets  ┌──────────────────┐
│ WM_FO    │──────────▶│ WM_Central   ── SQLite   │◀──────────│ WM_WS_M (Monitor)│
│ (FO_01..)│           │  · Dashboard web (HTTP)  │           │      ▲ socket    │
└────▲─────┘           │  · Kafka producer        │           │      ▼ local     │
     │                 └────────────┬─────────────┘           │ WM_WS_E (Engine) │
     │                              ▼                         └────────▲─────────┘
     │                ┌──────────────────────────┐                     │
     └──── consume ───│ KAFKA  topic wm-eventos  │──── consume ────────┘
         (telemetría) └──────────────────────────┘ (INICIO / ABORTO riego)
```

## Componentes

| Módulo | Fichero | Función |
|---|---|---|
| **WM_Central** | `CoreSystem/src/main/java/WM_Central.java` | Servidor de sockets (un hilo por cliente), lógica del sistema, BD SQLite, panel web y productor Kafka. |
| GestorRiegos | `CoreSystem/.../GestorRiegos.java` | Lógica de negocio: altas, ciclo de riego, averías, órdenes de CENTRAL. |
| WM_Central_Thread | `CoreSystem/.../WM_Central_Thread.java` | Atiende un socket: valida tramas (LRC), responde ACK/NACK, timeout de 15 s. |
| DatabaseManager | `CoreSystem/.../DatabaseManager.java` | SQLite: tablas `watering_stations` y `operarios`. |
| WebDashboard / Dashboard | `CoreSystem/.../WebDashboard.java`, `index.html` | Panel en tiempo real (SSE) con los 5 estados y las órdenes de CENTRAL. |
| KafkaManager | `CoreSystem/.../KafkaManager.java` | Publica los eventos en `wm-eventos` (clave = id de la WS). |
| **WM_WS_M** | `Water_Station/WM_WS_M.java` | Monitor: se registra en CENTRAL, supervisa al Engine (HEALTH_CHECK cada segundo), autoriza riegos y controla su duración. |
| **WM_WS_E** | `Water_Station/WM_WS_E.java` | Engine: electroválvula, caudalímetro simulado, detector de fugas (tecla K) y menú de la estación. |
| **WM_FO** | `Water_Station/WM_FO.java` | Operario: pide riegos (manual o desde fichero) y ve la telemetría de su WS. |

## Mecánica de un riego

1. `WM_FO` → CENTRAL: `ACTIVAR#WS_01#FO_01#20` (20 s).
2. CENTRAL comprueba en BD que la WS está **DISPONIBLE** y conectada → `SOLICITUD_RIEGO#<riegoId>#FO_01#20` al Monitor. El operario recibe cada paso (`PROCESANDO: ...`).
3. El Monitor comprueba que el Engine está sano → `RIEGO_AUTORIZADO#<riegoId>` (o `RIEGO_DENEGADO#<riegoId>#motivo`).
4. CENTRAL marca **REGANDO**, publica `INICIO_RIEGO#WS_01#FO_01#20#<riegoId>` en Kafka y avisa al operario (`AUTORIZADO: ...`).
5. El Engine consume el evento y **abre la electroválvula**. Cada segundo envía `TELEMETRIA#caudal#volumen` al Monitor → CENTRAL (panel) → Kafka (operario).
6. Fin: al cumplirse la duración (el Monitor ordena `FINALIZAR_RIEGO`) o con la opción **F** del menú de la estación. El Engine envía `FIN_RIEGO#volumen#duración`; CENTRAL vuelve a **DISPONIBLE** y manda el resumen al operario (`RESUMEN_FIN_RIEGO: ...`).
7. Fuga (tecla **K** en el Engine) durante el riego: la válvula se cierra al instante, el Monitor envía `AVERIA`, CENTRAL marca **FUGA**, avisa al operario (`ABORTADO: ...`) y publica `ABORTO_RIEGO`. Con **O** se resuelve (`RESUELTA`).

Órdenes de CENTRAL desde el panel web, a una estación o a todas: **Iniciar riego**, **Bloquear** (aborta el riego y pone *Fuera de Servicio*) y **Activar**.

### Protocolo de sockets

Trama: `<STX>CODIGO#campo1#...#campoN<ETX><LRC>` con `LRC = XOR` de los bytes de STX a ETX. CENTRAL responde `<ACK>` o `<NACK>motivo`.

| Origen → Destino | Mensajes |
|---|---|
| Monitor → CENTRAL | `REGISTRO#id#ubicación`, `PING#id`, `RIEGO_AUTORIZADO#riegoId`, `RIEGO_DENEGADO#riegoId#motivo`, `TELEMETRIA#id#caudal#vol`, `FIN_RIEGO#id#vol#dur`, `AVERIA#id`, `RESUELTA#id`, `SOLICITUD_MANUAL#id#dur` |
| CENTRAL → Monitor | `SOLICITUD_RIEGO#riegoId#operario#dur`, `ABORTAR_RIEGO#motivo`, `AVISO#texto` |
| Operario → CENTRAL | `REGISTRO_OPERARIO#id`, `ACTIVAR#ws#id#dur`, `LISTAR_WS#id`, `PING#id` |
| CENTRAL → Operario | Texto: `PROCESANDO:`, `AUTORIZADO:`, `DENEGADO:`, `ABORTADO:`, `RESUMEN_FIN_RIEGO:`, `LISTA_ESTACIONES:` |
| Monitor ↔ Engine (local) | `HEALTH_CHECK` → `OK`/`KO`; `TELEMETRIA`, `FIN_RIEGO`, `SOLICITUD_MANUAL`; `FINALIZAR_RIEGO`, `ABORTAR_RIEGO`, `INICIO_RIEGO` (respaldo), `AVISO` |

### Eventos Kafka (`wm-eventos`, clave = id de la WS)

`INICIO_RIEGO`, `TELEMETRIA`, `FIN_RIEGO`, `AVERIA`, `RESUELTA`, `ABORTO_RIEGO`, `BLOQUEO`, `ACTIVACION`.

## Robustez y tolerancia a fallos

1. **Reconexión automática**: Monitor y Operario reconectan con CENTRAL con backoff exponencial (2, 4, 8… máx. 30 s) y se vuelven a registrar; el Engine reintenta con su Monitor cada 2 s.
2. **Detección de caídas**: CENTRAL cierra un socket sin actividad durante 15 s y marca la WS como **DESCONECTADA**. Monitor y Operario envían `PING` cada 5 s si no hay tráfico, y detectan sockets semiabiertos (20 s sin respuesta).
3. **Válvula a prueba de fallos**: si el Engine pierde el enlace con el Monitor (error o 5 s sin mensajes) cierra la válvula. Si una WS cae regando, CENTRAL aborta el riego, avisa al operario y publica `ABORTO_RIEGO`. Si el Monitor pierde CENTRAL, aborta el riego local.
4. **ServerSocket del Monitor siempre abierto**: si el Engine se reinicia, el Monitor acepta la nueva conexión y reanuda los `HEALTH_CHECK`.
5. **Kafka**: el Engine consume `wm-eventos` con grupo persistente `ws-engine-<id>` e ignora eventos de más de 30 s (no reabre la válvula al reiniciar). La Central publica en un hilo aparte con cola acotada (si Kafka cae, los sockets siguen funcionando) y reintenta la conexión. Si Kafka no entrega `INICIO_RIEGO` en 6 s, el Monitor envía la orden por socket como respaldo.
6. **Persistencia en Railway**: la BD se guarda en `DB_PATH` (`/app/data/water_management.db` en Docker); montad un volumen de Railway en `/app/data`. Las estaciones bloqueadas siguen bloqueadas tras un reinicio.

Otras validaciones: los operarios deben estar dados de alta en la BD (por defecto `FO_01` … `FO_10`), se rechazan IDs duplicados con una conexión activa y se controla que dos operarios no pidan la misma WS a la vez.

## Despliegue

### Cloud (Railway): WM_Central + Kafka

1. Servicio **Kafka** (imagen `apache/kafka`) con un *TCP Proxy* para acceso externo; anunciad ese host:puerto en `KAFKA_ADVERTISED_LISTENERS`.
2. Servicio **Central** desde este repositorio (usa el `Dockerfile` raíz). Variables:
   - `KAFKA_BROKER` = dirección interna del broker (ej. `kafka.railway.internal:29092`)
   - `SOCKET_PORT` = `6000`
   - `DB_PATH` = `/app/data/water_management.db` + volumen montado en `/app/data`
3. Dominio público HTTP → puerto `PORT` (panel web). *TCP Proxy* → puerto `6000` (sockets).

En local se puede levantar todo con `docker compose up -d --build` en la raíz (Central en `localhost:6000`, web en `http://localhost:8080`, Kafka en `localhost:9092`).

### Laboratorio: estaciones y operarios (Docker)

```bash
cd Water_Station
# Crear .env con CENTRAL_HOST, CENTRAL_PORT y KAFKA_BROKER (ver compose.yml)
docker compose build
docker compose up -d monitor-ws1 engine-ws1 monitor-ws2 engine-ws2   # PC LAB 2
docker compose up -d fo1 fo2                                         # PC LAB 1
docker attach engine-ws1    # menú de la estación (Ctrl+P, Ctrl+Q para salir)
docker attach fo1           # menú del operario
```

Sin Docker (Windows, requiere JDK 21+): `Arrancar_Monitor.bat <host> <puerto> WS_01 "Parque Central" 7000`, `Arrancar_Engine.bat <broker> 7000 WS_01` y `Arrancar_Operario.bat <broker> FO_01 <host> <puerto>`.

### Menús

- **Engine** (`WM_WS_E`): `R [s]` riego manual · `F` finalizar riego · `K` simular fuga · `O` restablecer · `E` estado.
- **Operario** (`WM_FO`): 1 riego puntual · 2 servicios desde fichero (`ID_WS[,segundos]`, ver `servicios_ejemplo.txt`) · 3 listar estaciones · 4 salir.
