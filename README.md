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
| WebDashboard / Dashboard | `CoreSystem/.../WebDashboard.java`, `Dashboard.java` | Servidor HTTP del panel: ficheros estáticos, eventos en tiempo real (SSE, con latido cada 15 s) y API de órdenes y altas. |
| Panel web | `CoreSystem/src/main/resources/web/` | `index.html` + `css/styles.css` + módulos JS: `store.js` (estado y SSE), `map.js` (mapa por zonas), `stations.js` (tabla y listas), `detail.js` (panel lateral de una WS), `control.js` (órdenes globales y altas), `events.js` (historial), `api.js`, `ui.js`, `app.js` (pestañas). |
| KafkaManager | `CoreSystem/.../KafkaManager.java` | Publica los eventos en `wm-eventos` (clave = id de la WS). |
| **WM_WS_M** | `Water_Station/WM_WS_M.java` | Monitor: se registra en CENTRAL, supervisa al Engine (HEALTH_CHECK cada segundo), autoriza riegos y controla su duración. |
| **WM_WS_E** | `Water_Station/WM_WS_E.java` | Engine: electroválvula, caudalímetro simulado, detector de fugas (tecla K) y menú de la estación. |
| **WM_FO** | `Water_Station/WM_FO.java` | Operario: pide riegos (manual o desde fichero, también en modo automático) y ve la telemetría de su WS. |
| Protocolo | `Water_Station/Protocolo.java` | Tramas `<STX><DATA><ETX><LRC>` y ACK/NACK, común a Monitor, Engine y Operario. |

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

**Todos** los mensajes por socket (Operario ↔ CENTRAL, Monitor ↔ CENTRAL y Monitor ↔ Engine) van en tramas `<STX>CODIGO#campo1#...#campoN<ETX><LRC>` con `LRC = XOR` de los caracteres de STX a ETX. Quien recibe una trama comprueba formato y LRC y responde `<ACK>` si es correcta o `<NACK>[motivo]` si no (los ACK/NACK no se responden). En CENTRAL lo implementa `WM_Central_Thread`; en las estaciones y el operario, la clase `Protocolo`.

| Origen → Destino | Mensajes |
|---|---|
| Monitor → CENTRAL | `REGISTRO#id#ubicación[#riegoId#operario#dur]`, `PING#id`, `RIEGO_AUTORIZADO#riegoId`, `RIEGO_DENEGADO#riegoId#motivo`, `TELEMETRIA#id#caudal#vol`, `FIN_RIEGO#id#vol#dur#riegoId#motivo`, `AVERIA#id`, `RESUELTA#id`, `SOLICITUD_MANUAL#id#dur`, `SINCRONIZADO#id` |
| CENTRAL → Monitor | `SOLICITUD_RIEGO#riegoId#operario#dur`, `ABORTAR_RIEGO#motivo`, `CONFIRMACION_FIN#riegoId`, `AVISO#texto` |
| Operario → CENTRAL | `REGISTRO_OPERARIO#id`, `ACTIVAR#ws#id#dur`, `LISTAR_WS#id`, `PING#id` |
| CENTRAL → Operario | Texto: `PROCESANDO:`, `AUTORIZADO:`, `REANUDADO:`, `DENEGADO:`, `ABORTADO:`, `RESUMEN_FIN_RIEGO:`, `LISTA_ESTACIONES:`, `SERVICIO_EN_CURSO:`, `AVISO:` |
| Monitor ↔ Engine (local) | `HEALTH_CHECK` → `OK`/`KO`; `TELEMETRIA`, `FIN_RIEGO`, `SOLICITUD_MANUAL`; `FINALIZAR_RIEGO`, `ABORTAR_RIEGO`, `INICIO_RIEGO` (respaldo), `AVISO` |

### Eventos Kafka (`wm-eventos`, clave = id de la WS)

`INICIO_RIEGO`, `TELEMETRIA`, `FIN_RIEGO`, `AVERIA`, `RESUELTA`, `ABORTO_RIEGO`, `BLOQUEO`, `ACTIVACION`.

## Robustez y tolerancia a fallos

1. **Reconexión automática**: Monitor y Operario reconectan con CENTRAL con backoff exponencial (2, 4, 8… máx. 30 s) y se vuelven a registrar; el Engine reintenta con su Monitor cada 2 s.
2. **Detección de caídas**: CENTRAL cierra un socket sin actividad durante 15 s y marca la WS como **DESCONECTADA**. Monitor y Operario envían `PING` cada 5 s si no hay tráfico, y detectan sockets semiabiertos (20 s sin respuesta).
3. **Válvula a prueba de fallos**: si el Engine pierde el enlace con el Monitor (error o 5 s sin mensajes) cierra la válvula y muestra **ESTACIÓN AVERIADA** (no admite riegos ni manuales). Si una WS cae regando, CENTRAL aborta el riego, avisa al operario y publica `ABORTO_RIEGO`.
4. **ServerSocket del Monitor siempre abierto**: si el Engine se reinicia, el Monitor acepta la nueva conexión y reanuda los `HEALTH_CHECK`.
5. **Kafka**: el Engine consume `wm-eventos` con grupo persistente `ws-engine-<id>` e ignora eventos de más de 30 s (no reabre la válvula al reiniciar). La Central publica en un hilo aparte con cola acotada (si Kafka cae, los sockets siguen funcionando) y reintenta la conexión. Si Kafka no entrega `INICIO_RIEGO` en 6 s, el Monitor envía la orden por socket como respaldo.
6. **Persistencia en Railway**: la BD se guarda en `DB_PATH` (`/app/data/water_management.db` en Docker); montad un volumen de Railway en `/app/data`. Las estaciones bloqueadas siguen bloqueadas tras un reinicio.

### Ninguna transacción se pierde (caídas de la guía de corrección)

| Cae… | Qué pasa |
|---|---|
| **WM_Central** | Las WS siguen regando hasta terminar; no se admiten nuevos riegos ("Imposible conectar con WM_Central" en FO, Monitor y Engine). Los `FIN_RIEGO` quedan guardados en el Monitor y se reenvían al reconectar hasta que CENTRAL responde `CONFIRMACION_FIN`. Los riegos están en SQLite (tabla `riegos`), así que una CENTRAL reiniciada reconoce el riego: si sigue en marcha, la WS lo declara al registrarse (`REGISTRO#id#ubic#riegoId#operario#dur`) y vuelve a REGANDO. |
| **Un operario (FO)** | El riego sigue. Los mensajes que no se le pudieron entregar se guardan en su buzón (tabla `mensajes_pendientes`) y los recibe al volver; si el riego sigue en curso, vuelve a ver su telemetría. |
| **El Engine** | El Monitor notifica la avería (WS en rojo), CENTRAL avisa al operario y el Monitor guarda lo regado (última telemetría). Al recuperarse el Engine, se envía el estado final del riego y el operario recibe el resumen. |
| **El Monitor** | CENTRAL marca la WS como DESCONECTADA y avisa al operario. El Engine cierra la válvula, muestra ESTACIÓN AVERIADA y guarda el estado final, que entrega al Monitor cuando vuelve. |

Todo cierre de válvula genera `FIN_RIEGO#ws#volumen#duración#riegoId#motivo` (`TIEMPO`, `MANUAL`, `ABORTADO`, `AVERIA`, `ENLACE`); CENTRAL lo procesa una sola vez aunque llegue repetido.

### Autenticación y altas

- **Estaciones**: solo se admiten las dadas de alta en la BD de CENTRAL (por defecto `WS_01` … `WS_10`, repartidas por las zonas del mapa). Cualquier otra recibe `NACK WS_NO_REGISTRADA` y su Monitor muestra "Autenticación DENEGADA".
- **Operarios**: por defecto `FO_01` … `FO_10`; uno no registrado es rechazado y la aplicación se cierra.
- **Altas nuevas**: desde el panel web (*Network registry*) o con `GET /api/alta?tipo=ws&id=WS_11&loc=Plaza%20Mayor` / `GET /api/alta?tipo=op&id=FO_11`.
- Se rechazan IDs duplicados con una conexión activa y se controla que dos operarios no pidan la misma WS a la vez.

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

Sin Docker (Windows, solo requiere **Java 11+**, no se compila nada): los `.jar` ya generados (`WM_WS_M.jar`, `WM_WS_E.jar`, `WM_FO.jar`) se lanzan con los scripts, que preguntan el ID al arrancar y usan por defecto las direcciones de Railway:

```bat
Arrancar_Monitor.bat     :: ID de la WS, ubicación (Enter = la de CENTRAL), puerto local
Arrancar_Engine.bat      :: ID de la WS y puerto de su Monitor
Arrancar_Operario.bat    :: ID del operario y fichero de servicios (Enter = solo menú)
```

También se pueden pasar como parámetros, p. ej. `Arrancar_Operario.bat iriguchi.proxy.rlwy.net:38689 FO_01 monorail.proxy.rlwy.net 32265 servicios_ejemplo.txt`, o directamente `java -jar WM_FO.jar <broker> FO_01 <host> <puerto> [fichero]`.

**Modo automático (sin interacción)**: si el operario recibe un fichero de servicios, al conectarse los ejecuta uno tras otro (4 s entre servicios) y después muestra el menú. `compilar.bat` regenera los `.jar` (solo para desarrollo).

### Menús

- **Engine** (`WM_WS_E`): `R [s]` riego manual · `F` finalizar riego · `K` simular fuga · `O` restablecer · `E` estado.
- **Operario** (`WM_FO`): 1 riego puntual · 2 servicios desde fichero (`ID_WS[,segundos]`, ver `servicios_ejemplo.txt`) · 3 listar estaciones · 4 salir.
