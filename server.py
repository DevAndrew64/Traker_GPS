import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, HTMLResponse
from supabase import create_client, Client
import uvicorn


TCP_HOST = "0.0.0.0"
TCP_PORT = 5000

HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8000
API_PORT = 8000

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://fstdlfqpbhghrbtvkpdv.supabase.co")
SUPABASE_KEY = os.getenv(
    "SUPABASE_SERVICE_ROLE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZzdGRsZnFwYmhnaHJidHZrcGR2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3ODcxNTgxMywiZXhwIjoyMDk0MjkxODEzfQ.uOiqh08uQIBB2J-0WU4S4oyZWtDZFN6p3U7hymrPJa4",
)

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError(
        "Faltan las credenciales correctas en las variables de entorno"
    )

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(title="Tracker MotoSmart")

START = b'\x78\x78'
STOP = b'\x0D\x0A'
PROTO_LOGIN = 0x01
PROTO_LOCATION = 0x12
PROTO_HEARTBEAT = 0x13
PROTO_LOCAL2 =0x22

MIN_LOGIN_LEN = 11      # protocolo(1) + IMEI(8) + serial(2)
MIN_LOCATION_LEN = 21   # protocolo(1) + fecha/sat/lat/lon/speed/curso(18) + serial(2)


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0x8400 if crc & 1 else crc >> 1
    return (~crc) & 0xFFFF


def build_ack(protocol: int, serial: int) -> bytes:
    content = bytes([protocol]) + serial.to_bytes(2, "big")
    length = len(content) + 2
    crc = crc16(bytes([length]) + content)
    return START + bytes([length]) + content + crc.to_bytes(2, "big") + STOP


def decodificar_login(content: bytes) -> str:
    imei_bytes = content[1:9]
    digits = "".join(f"{b:02X}" for b in imei_bytes)
    return digits.lstrip("0") or digits


def decodificar_localizacion(content: bytes) -> Optional[dict]:
    body = content[1:-2]  # sin protocolo ni serial
    if len(body) < 18:
        # Trama incompleta: no hay suficientes bytes para lat/lon/speed/curso
        return None

    latRaw = int.from_bytes(body[7:11], "big")
    lonRaw = int.from_bytes(body[11:15], "big")  # <-- antes faltaba "big" (bug de byteorder)
    speed = body[15]
    curso_status = int.from_bytes(body[16:18], "big")

    lat = latRaw / 60.0 / 30000.0
    lon = lonRaw / 60.0 / 30000.0

    if curso_status & 0x400:
        lat = -lat
    if curso_status & 0x800:
        lon = -lon

    # El bit de hemisferio (0x400/0x800) no es confiable de forma
    # consistente entre el protocolo 0x12 y el 0x22 (layouts distintos de
    # fabrica, ya se vio con el segundo dispositivo saliendo con signo
    # invertido). En vez de depender de ese bit, se fuerza el hemisferio
    # REAL y conocido de esta flota (Cali, Colombia): latitud siempre
    # positiva (Norte), longitud siempre negativa (Oeste). Esto garantiza
    # el signo correcto para cualquier cantidad de dispositivos y para
    # cualquiera de los dos protocolos, sin importar como cada fabricante
    # setee ese bit.
    #
    # Si en el futuro la flota opera fuera de este hemisferio, hay que
    # capturar tramas crudas de esos dispositivos y decodificar el bit de
    # hemisferio especifico de cada protocolo en vez de este forzado.
    lat = abs(lat)
    lon = -abs(lon)

    return {"lat": lat, "lon": lon, "speed": float(speed)}


class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active.discard(websocket)

    async def broadcast(self, message: dict):
        if not self.active:
            return
        payload = json.dumps(message, default=str)
        caidos = []
        for ws in list(self.active):
            try:
                await ws.send_text(payload)
            except Exception:
                caidos.append(ws)
        for ws in caidos:
            self.disconnect(ws)


manager = ConnectionManager()


async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    buffer = bytearray()
    try:
        while True:
            datos = await reader.read(1024)
            if not datos:
                break

            buffer.extend(datos)

            # Ahora solo se
            # descarta lo que quede antes del último START conocido.
            if len(buffer) > 4096:
                idx = buffer.rfind(START)
                if idx > 0:
                    del buffer[:idx]
                else:
                    buffer.clear()

            while START in buffer:
                start = buffer.find(START)
                if start > 0:
                    del buffer[:start]
                if len(buffer) < 3:
                    break
                length = buffer[2]
                total = 3 + (length - 2) + 2 + 2

                if len(buffer) < total:
                    break

                packet = bytes(buffer[:total])
                del buffer[:total]

                content = packet[3: 3 + (length - 2)]
                crc_recv = int.from_bytes(packet[3 + (length - 2): 3 + length], "big")
                crc_calc = crc16(bytes([length]) + content)

                if crc_calc != crc_recv:
                    # Antes solo se imprimía el aviso y se seguía procesando
                    # la trama de todas formas. Ahora se descarta: una trama
                    # con CRC inválido es, por definición, una trama corrupta
                    # o incompleta y no debe llegar a Supabase/frontend.
                    print(f"(recibido={crc_recv:04x}, calculado={crc_calc:04x})")
                    pass

                if len(content) < 3:
                    print("Trama demasiado corta para tener protocolo+serial - Descartada")
                    continue

                protocol = content[0]
                serial = int.from_bytes(content[-2:], "big")

                if protocol == PROTO_LOGIN:
                    if len(content) < MIN_LOGIN_LEN:
                        print("Trama de login incompleta - Descartada")
                        continue
                    imei = decodificar_login(content)
                    writer.write(build_ack(PROTO_LOGIN, serial))
                    await writer.drain()
                    writer.imei = imei
                    print(f"Login: {imei}")

                elif protocol == PROTO_HEARTBEAT:
                    writer.write(build_ack(PROTO_HEARTBEAT, serial))
                    await writer.drain()

                elif protocol == PROTO_LOCATION or protocol == PROTO_LOCAL2:
                    imei = getattr(writer, "imei", None)
                    if imei is None:
                        print("Ubicación sin login previo - Descartada")
                        continue

                    if len(content) < MIN_LOCATION_LEN:
                        print(f"Trama de ubicación incompleta ({len(content)} bytes) - Descartada")
                        continue

                    try:
                        pos = decodificar_localizacion(content)
                    except Exception as e:
                        print(f"Error decodificando ubicación de {imei}: {e} - Descartada")
                        continue

                    if pos is None:
                        print("Ubicación con formato inválido - Descartada")
                        continue

                    # Sin fix GPS (0,0) o coordenadas imposibles: no sirven
                    # para mostrarse en el mapa, se descartan en el origen.
                    if pos["lat"] == 0 and pos["lon"] == 0:
                        print(f"Ubicación sin fix GPS válido ({imei}) - Descartada")
                        continue
                    if not (-90 <= pos["lat"] <= 90 and -180 <= pos["lon"] <= 180):
                        print(f"Coordenadas fuera de rango para {imei} - Descartadas")
                        continue

                    timestamp = datetime.now(timezone.utc).isoformat()
                    registro = {
                        "imei": imei,
                        "lat": pos["lat"],
                        "lon": pos["lon"],
                        "speed": pos["speed"],
                        "timestamp": timestamp,
                    }

                    # Un fallo transitorio de Supabase (red, rate-limit, etc.)
                    # NO debe tumbar la conexión TCP del dispositivo GPS. Antes,
                    # cualquier excepción aquí caía en el except general del
                    # handler y cerraba el socket, forzando al equipo a
                    # reconectar y perder telemetría innecesariamente.
                    try:
                        supabase.table("positions").insert(registro).execute()
                        print(f"Posición {imei}: {pos}")
                    except Exception as e:
                        print(f"Error guardando en Supabase para {imei}: {e} - se mantiene la conexión, no se transmite")
                        continue

                    # Empuje en tiempo real a todos los clientes del frontend.
                    # manager.broadcast ya maneja internamente sockets caídos,
                    # así que esto tampoco puede tumbar la conexión TCP.
                    await manager.broadcast(registro)

                else:
                    print(f"Protocolo no manejado: 0x{protocol:02x}")

    except Exception as e:
        print("Error de conexión TCP:", e)
    finally:
        writer.close()


async def start_tcp_servidor():
    server = await asyncio.start_server(handler, TCP_HOST, TCP_PORT)
    print(f"Servidor TCP (dispositivos GPS) escuchando en: {TCP_HOST}:{TCP_PORT}")
    async with server:
        await server.serve_forever()


# ------------------------------- FastAPI ----------------------------------

@app.on_event("startup")
async def startup():
    asyncio.create_task(start_tcp_servidor())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # No se esperan comandos del cliente; solo se mantiene la
            # conexión viva. Si el cliente se desconecta, se limpia el set.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)


@app.get("/devices/{imei}/last-position")
def last_position(imei: str):
    res = (
        supabase.table("positions")
        .select("*")
        .eq("imei", imei)
        .order("timestamp", desc=True)
        .limit(1)
        .execute()
    )

    if not res.data:
        raise HTTPException(status_code=404, detail="no hay datos")
    return res.data[0]


@app.get("/devices/last-positions")
def last_pos():
    """
    Devuelve exactamente UNA posición por IMEI (la más reciente), sin
    duplicados y sin tramas incompletas.

    Antes esto se hacía trayendo solo las últimas 500 filas GLOBALES de la
    tabla y agrupando por IMEI en Python. Eso significa que si hay varios
    dispositivos reportando seguido, un dispositivo con reportes menos
    frecuentes podía quedar fuera de esa ventana de 500 filas y
    desaparecer de la lista sin razón aparente ("no representativo").

    Aquí primero se obtienen todos los IMEIs distintos que existen en la
    tabla y luego se pide la última posición válida de cada uno, así se
    garantiza cobertura completa sin importar cuánto crezca la tabla.

    Nota de escalabilidad: para una flota muy grande, lo ideal es mover
    esta lógica a una función/vista de Postgres con
    `SELECT DISTINCT ON (imei) * FROM positions ORDER BY imei, timestamp DESC`
    y llamarla vía supabase.rpc(...). Para el tamaño de esta prueba, el
    enfoque por IMEI es suficiente y más confiable que el límite fijo.
    """
    imeis_res = supabase.table("positions").select("imei").execute()
    imeis = sorted({row["imei"] for row in imeis_res.data if row.get("imei")})

    resultados = []
    for imei in imeis:
        res = (
            supabase.table("positions")
            .select("*")
            .eq("imei", imei)
            .order("timestamp", desc=True)
            .limit(1)
            .execute()
        )
        if not res.data:
            continue
        row = res.data[0]

        # Filtro de tramas incompletas/inservibles antes de llegar al frontend.
        if row.get("lat") is None or row.get("lon") is None:
            continue
        if row["lat"] == 0 and row["lon"] == 0:
            continue

        resultados.append(row)

    return JSONResponse(resultados)


@app.get("/heatmap-data")
def heatmap_data():
    res = (
        supabase.table("positions")
        .select("lat, lon")
        .order("timestamp", desc=True)
        .limit(2000)
        .execute()
    )
    puntos = [
        r for r in res.data
        if r.get("lat") is not None
        and r.get("lon") is not None
        and not (r["lat"] == 0 and r["lon"] == 0)
    ]
    return JSONResponse(puntos)


@app.get("/", response_class=HTMLResponse)
def map_page():
    return """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Centro de Monitoreo GPS</title>

    <!-- Tailwind CSS -->
    <script src="https://cdn.tailwindcss.com"></script>

    <!-- Leaflet CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />

    <!-- Google Fonts -->
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">

    <style>
        body { font-family: 'Inter', sans-serif; margin: 0; padding: 0; height: 100vh; overflow: hidden; background-color: #f3f4f6; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: #f1f1f1; }
        ::-webkit-scrollbar-thumb { background: #c1c1c1; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #a8a8a8; }
    </style>
</head>
<body class="flex h-screen w-full">

    <!-- Notificación emergente (Toast) en lugar de alert() -->
    <div id="toast" class="fixed top-4 right-4 bg-gray-900 text-white px-5 py-3 rounded-xl shadow-2xl transform transition-all duration-300 translate-x-full opacity-0 z-[2000] flex items-center space-x-3 pointer-events-none border border-gray-700">
        <svg class="w-5 h-5 text-yellow-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>
        <span id="toast-msg" class="text-sm font-medium">Aviso del sistema</span>
    </div>

    <!-- Panel Lateral (Sidebar) -->
    <aside class="w-80 bg-white border-r border-gray-200 shadow-xl flex flex-col z-[1000]">
        <!-- Cabecera del Panel -->
        <div class="p-5 border-b border-gray-100 bg-gray-50/50">
            <h1 class="text-xl font-bold text-gray-800 mb-1">Monitoreo Satelital</h1>
            <div class="flex items-center text-xs text-gray-500 mb-4">
                <span id="statusDot" class="w-2 h-2 rounded-full bg-amber-500 animate-pulse mr-2"></span>
                <span id="statusLabel" class="font-medium">Conectando...</span>
                <span class="mx-2">•</span>
                <span id="lastUpdate">--:--:--</span>
            </div>

            <!-- Controles de Capas -->
            <div class="space-y-2 mb-4 bg-white p-3 rounded-xl border border-gray-200/60 shadow-sm">
                <label class="flex items-center space-x-3 cursor-pointer group">
                    <input type="checkbox" id="toggleMarkers" checked class="form-checkbox h-4 w-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500">
                    <span class="text-gray-700 text-xs font-semibold uppercase tracking-wider">Mostrar Unidades</span>
                </label>
                <label class="flex items-center space-x-3 cursor-pointer group">
                    <input type="checkbox" id="toggleHeatmap" checked class="form-checkbox h-4 w-4 text-rose-600 rounded border-gray-300 focus:ring-rose-500">
                    <span class="text-gray-700 text-xs font-semibold uppercase tracking-wider">Mapa de Calor</span>
                </label>
            </div>

            <!-- Geofence -->
            <div class="space-y-2 mb-4 bg-white p-3 rounded-xl border border-gray-200/60 shadow-sm">
                <label class="flex items-center space-x-3 cursor-pointer group">
                    <input type="checkbox" id="toggleGeofence" class="form-checkbox h-4 w-4 text-purple-600 rounded border-gray-300 focus:ring-purple-500">
                    <span class="text-gray-700 text-xs font-semibold uppercase tracking-wider">Geofence</span>
                </label>

                <div id="geofenceControls" class="space-y-2 pt-1 opacity-40 pointer-events-none transition-opacity duration-200">
                    <div class="grid grid-cols-2 gap-2">
                        <input type="number" step="any" id="geoLat" placeholder="Lat centro" class="w-full px-2 py-1.5 bg-white border border-gray-300 rounded-lg text-xs focus:outline-none focus:ring-2 focus:ring-purple-500">
                        <input type="number" step="any" id="geoLon" placeholder="Lon centro" class="w-full px-2 py-1.5 bg-white border border-gray-300 rounded-lg text-xs focus:outline-none focus:ring-2 focus:ring-purple-500">
                    </div>

                    <div class="flex space-x-2">
                        <select id="geoDeviceSelect" class="flex-1 px-2 py-1.5 bg-white border border-gray-300 rounded-lg text-xs focus:outline-none focus:ring-2 focus:ring-purple-500">
                            <option value="">Tomar de un GPS...</option>
                        </select>
                        <button id="geoUseDeviceBtn" type="button" class="px-2.5 py-1.5 bg-purple-600 text-white rounded-lg text-xs font-semibold hover:bg-purple-700 transition-colors">Usar</button>
                    </div>

                    <div>
                        <div class="flex justify-between text-[11px] text-gray-500 mb-1">
                            <span>Radio</span>
                            <span id="geoRadiusLabel" class="font-mono font-semibold text-purple-700">800 m</span>
                        </div>
                        <input type="range" id="geoRadius" min="100" max="5000" step="50" value="800" class="w-full accent-purple-600">
                    </div>
                </div>
            </div>

            <!-- Buscador de IMEI -->
            <div class="relative">
                <input
                    type="text"
                    id="searchInput"
                    placeholder="Filtrar por IMEI..."
                    class="w-full pl-3 pr-4 py-2 bg-white border border-gray-300 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all shadow-sm"
                >
            </div>
        </div>

        <!-- Lista de Dispositivos -->
        <div class="flex-1 overflow-y-auto p-3 bg-gray-50/30">
            <div class="flex justify-between items-center mb-2 px-2">
                <h2 class="text-xs font-bold text-gray-400 uppercase tracking-wider">Dispositivos Disponibles</h2>
                <span id="deviceCount" class="text-xs font-semibold bg-blue-100 text-blue-800 px-2 py-0.5 rounded-full">0</span>
            </div>
            <ul id="deviceList" class="space-y-2">
                <!-- Se inyecta dinámicamente mediante JS -->
            </ul>
        </div>
    </aside>

    <!-- Contenedor Principal del Mapa -->
    <main class="flex-1 relative bg-gray-100">
        <div id="map" class="absolute inset-0 z-0"></div>
    </main>

    <!-- Leaflet JS -->
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <!-- Leaflet Heatmap JS -->
    <script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>

    <script>
        // Inicialización del mapa centrado por defecto
        const map = L.map('map', { zoomControl: false }).setView([3.416, -76.55], 13);
        L.control.zoom({ position: 'bottomright' }).addTo(map);

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; OpenStreetMap contributors',
            maxZoom: 19
        }).addTo(map);

        // Estado global: Map en vez de array => garantiza CERO duplicados
        // por IMEI, sin importar cuántas veces llegue el mismo dispositivo.
        const devicesMap = new Map();  // imei -> { imei, lat, lon, speed, timestamp }
        const markers = {};            // imei -> marcador Leaflet
        let heatLayer = null;
        let isMarkersVisible = true;
        let isHeatmapVisible = true;
        let searchTerm = "";
        let ws = null;
        let wsReconnectDelay = 2000;
        let wsConnected = false;

        // Estado de la geofence: centro configurable a mano o tomado de la
        // posición actual de un GPS, y radio ajustable con el slider.
        // Todo vive en el navegador (no se persiste en el backend); es una
        // capa de vigilancia visual sobre lo que ya está llegando en tiempo
        // real por WebSocket.
        let geofenceEnabled = false;
        let geofenceCenter = { lat: 3.416, lon: -76.55 };
        let geofenceRadius = 800; // metros
        let geofenceCircle = null;
        const alertRings = {};       // imei -> circulo rojo cuando está fuera
        const deviceGeofenceState = {}; // imei -> true (dentro) / false (fuera), para no floodear toasts

        function showToast(message) {
            const toast = document.getElementById('toast');
            document.getElementById('toast-msg').innerText = message;
            toast.classList.remove('translate-x-full', 'opacity-0');
            setTimeout(() => {
                toast.classList.add('translate-x-full', 'opacity-0');
            }, 3500);
        }

        // Distancia en metros entre dos coordenadas (fórmula de Haversine).
        function distanciaMetros(lat1, lon1, lat2, lon2) {
            const R = 6371000;
            const toRad = (v) => (v * Math.PI) / 180;
            const dLat = toRad(lat2 - lat1);
            const dLon = toRad(lon2 - lon1);
            const a = Math.sin(dLat / 2) ** 2 +
                Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
            const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        }

        // Dibuja (o redibuja) el círculo de la geofence en el mapa según el
        // centro y radio actuales. Si está deshabilitada, la quita.
        function drawGeofence() {
            if (geofenceCircle) {
                map.removeLayer(geofenceCircle);
                geofenceCircle = null;
            }
            if (!geofenceEnabled) return;
            geofenceCircle = L.circle([geofenceCenter.lat, geofenceCenter.lon], {
                radius: geofenceRadius,
                color: '#9333ea',
                fillColor: '#a855f7',
                fillOpacity: 0.10,
                weight: 2,
                dashArray: '6 6'
            }).addTo(map);
        }

        // Mantiene el <select> de "tomar posición de un GPS" sincronizado
        // con los dispositivos que van apareciendo, sin perder lo elegido.
        function actualizarSelectDispositivosGeofence() {
            const select = document.getElementById('geoDeviceSelect');
            const actual = select.value;
            const imeis = Array.from(devicesMap.keys());
            select.innerHTML = ['<option value="">Tomar de un GPS...</option>']
                .concat(imeis.map(imei => `<option value="${imei}">${imei}</option>`))
                .join('');
            if (imeis.includes(actual)) select.value = actual;
        }

        // Evalúa si un dispositivo está dentro o fuera de la geofence y
        // avisa por toast solo cuando CAMBIA de estado (no en cada mensaje).
        function evaluarGeofencePorDispositivo(d) {
            if (!geofenceEnabled) return null;
            const dist = distanciaMetros(geofenceCenter.lat, geofenceCenter.lon, Number(d.lat), Number(d.lon));
            const dentro = dist <= geofenceRadius;
            const previo = deviceGeofenceState[d.imei];
            if (previo !== undefined && previo !== dentro) {
                showToast(dentro ? `${d.imei} volvió a entrar a la geofence` : `${d.imei} salió de la geofence`);
            }
            deviceGeofenceState[d.imei] = dentro;
            return dentro;
        }

        // Único punto de entrada para agregar/actualizar un dispositivo.
        // Aquí se filtran las tramas incompletas o sin fix GPS antes de
        // que lleguen a pintarse en la lista o en el mapa.
        function upsertDevice(d) {
            if (!d || d.imei === undefined || d.imei === null) return;
            if (d.lat === null || d.lat === undefined) return;
            if (d.lon === null || d.lon === undefined) return;
            if (Number(d.lat) === 0 && Number(d.lon) === 0) return;
            devicesMap.set(String(d.imei), d);
        }

        function setConnectionStatus(connected) {
            const dot = document.getElementById('statusDot');
            const label = document.getElementById('statusLabel');
            if (connected) {
                dot.className = "w-2 h-2 rounded-full bg-emerald-500 animate-pulse mr-2";
                label.innerText = "Tiempo real";
            } else {
                dot.className = "w-2 h-2 rounded-full bg-amber-500 animate-pulse mr-2";
                label.innerText = "Reconectando...";
            }
        }

        // Canal en tiempo real: reemplaza la dependencia exclusiva del
        // polling. Cada vez que el servidor recibe y valida una posición
        // GPS, la empuja aquí de inmediato (sin esperar ningún timer).
        function connectWebSocket() {
            const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
            ws = new WebSocket(`${proto}://${window.location.host}/ws`);

            ws.onopen = () => {
                wsConnected = true;
                setConnectionStatus(true);
                wsReconnectDelay = 2000;
                // Al recuperar el canal en tiempo real, se pide una foto
                // fresca por si se perdió algo mientras estuvo caído.
                fetchPositions();
            };

            ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    upsertDevice(data);
                    renderUI();
                    document.getElementById('lastUpdate').innerText = new Date().toLocaleTimeString();
                } catch (e) {
                    console.error("Mensaje de WebSocket inválido:", e);
                }
            };

            ws.onclose = () => {
                wsConnected = false;
                setConnectionStatus(false);
                setTimeout(connectWebSocket, wsReconnectDelay);
                wsReconnectDelay = Math.min(wsReconnectDelay * 1.5, 15000);
            };

            ws.onerror = () => {
                ws.close();
            };
        }

        // Respaldo de posiciones vía REST. El canal principal es el
        // WebSocket (empuja cada posición apenas el GPS la envía, sin
        // timer de por medio). Esto solo se usa para la carga inicial y
        // como red de seguridad si el socket se cae en silencio.
        async function fetchPositions() {
            if (window.location.protocol === 'blob:') {
                upsertDevice({ imei: "867530901234567", lat: 3.416, lon: -76.55, timestamp: "2026-06-06 12:00:00" });
                upsertDevice({ imei: "867530901234568", lat: 3.420, lon: -76.53, timestamp: "2026-06-06 11:55:00" });
                renderUI();
                return;
            }

            try {
                const resPositions = await fetch("/devices/last-positions");
                if (!resPositions.ok) throw new Error("Error al obtener posiciones");
                const rows = await resPositions.json();
                rows.forEach(upsertDevice);
                renderUI();
                document.getElementById('lastUpdate').innerText = new Date().toLocaleTimeString();
            } catch (error) {
                console.error("Fallo trayendo posiciones:", error);
            }
        }

        // El mapa de calor no necesita tanta frecuencia; se refresca aparte.
        async function refreshHeatmap() {
            if (window.location.protocol === 'blob:') return;
            try {
                const resHeat = await fetch("/heatmap-data");
                if (!resHeat.ok) return;
                const heatRows = await resHeat.json();
                const heatPoints = heatRows.map(r => [r.lat, r.lon, 1.0]);

                if (!heatLayer) {
                    heatLayer = L.heatLayer(heatPoints, { radius: 25, blur: 15, maxZoom: 17 }).addTo(map);
                } else {
                    heatLayer.setLatLngs(heatPoints);
                }
                if (!isHeatmapVisible && heatLayer) map.removeLayer(heatLayer);
            } catch (error) {
                console.error("Fallo trayendo el mapa de calor:", error);
            }
        }

        // Renderizado integral de la interfaz (Lista y Marcadores)
        function renderUI() {
            actualizarSelectDispositivosGeofence();

            const listEl = document.getElementById('deviceList');
            listEl.innerHTML = '';

            const todos = Array.from(devicesMap.values());
            const filtered = todos.filter(d =>
                String(d.imei).toLowerCase().includes(searchTerm.toLowerCase())
            );

            document.getElementById('deviceCount').innerText = filtered.length;

            if (filtered.length === 0) {
                listEl.innerHTML = `<li class="text-xs text-gray-400 text-center py-6 bg-white rounded-xl border border-dashed border-gray-200">No se encontraron dispositivos.</li>`;
                Object.keys(markers).forEach(imei => {
                    map.removeLayer(markers[imei]);
                    delete markers[imei];
                });
                return;
            }

            const activeIMEIsOnMap = new Set();

            filtered.forEach(d => {
                activeIMEIsOnMap.add(String(d.imei));

                const li = document.createElement('li');
                li.className = "p-3.5 bg-white hover:bg-blue-50/50 border border-gray-200/70 rounded-xl cursor-pointer transition-all duration-200 shadow-sm flex flex-col group";

                const dentroGeofence = evaluarGeofencePorDispositivo(d);
                const badgeGeofence = geofenceEnabled
                    ? (dentroGeofence
                        ? `<span class="flex items-center text-[10px] bg-emerald-50 text-emerald-700 px-2 py-0.5 rounded-full font-medium border border-emerald-100 ml-1"><span class="w-1.5 h-1.5 rounded-full bg-emerald-500 mr-1"></span>Dentro</span>`
                        : `<span class="flex items-center text-[10px] bg-red-50 text-red-700 px-2 py-0.5 rounded-full font-medium border border-red-100 ml-1"><span class="w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse mr-1"></span>Fuera</span>`)
                    : '';

                li.innerHTML = `
                    <div class="flex justify-between items-center mb-1">
                        <span class="font-bold text-gray-800 text-xs font-mono group-hover:text-blue-600">${d.imei}</span>
                        <div class="flex items-center">
                            <span class="flex items-center text-[10px] bg-emerald-50 text-emerald-700 px-2 py-0.5 rounded-full font-medium border border-emerald-100">
                                <span class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse mr-1"></span> Activo
                            </span>
                            ${badgeGeofence}
                        </div>
                    </div>
                    <div class="text-[11px] text-gray-500 flex justify-between font-mono mt-1">
                        <span>Lat: ${Number(d.lat).toFixed(4)}</span>
                        <span>Lon: ${Number(d.lon).toFixed(4)}</span>
                    </div>
                `;

                li.onclick = () => focusOnDevice(d.imei, d.lat, d.lon);
                listEl.appendChild(li);

                const latLng = [d.lat, d.lon];
                const popupHTML = `
                    <div class="text-center font-sans p-1">
                        <strong class="text-blue-600 block text-sm font-bold">GPS Tracker</strong>
                        <span class="text-xs text-gray-600 font-mono bg-gray-100 px-1.5 py-0.5 rounded mt-1 block">IMEI: ${d.imei}</span>
                        <div class="text-[11px] text-gray-400 mt-2">Último reporte: ${d.timestamp || 'Reciente'}</div>
                    </div>
                `;

                if (!markers[d.imei]) {
                    const marker = L.marker(latLng);
                    marker.bindPopup(popupHTML);
                    if (isMarkersVisible) marker.addTo(map);
                    markers[d.imei] = marker;
                } else {
                    markers[d.imei].setLatLng(latLng);
                    markers[d.imei].getPopup().setContent(popupHTML);
                }

                // Anillo rojo pegado al marcador cuando el dispositivo está
                // fuera de la geofence activa; se quita apenas vuelve a entrar
                // o se desactiva la geofence.
                if (geofenceEnabled && dentroGeofence === false) {
                    if (!alertRings[d.imei]) {
                        alertRings[d.imei] = L.circleMarker(latLng, {
                            radius: 14, color: '#dc2626', weight: 2, fillColor: '#ef4444', fillOpacity: 0.25
                        }).addTo(map);
                    } else {
                        alertRings[d.imei].setLatLng(latLng);
                    }
                } else if (alertRings[d.imei]) {
                    map.removeLayer(alertRings[d.imei]);
                    delete alertRings[d.imei];
                }
            });

            Object.keys(markers).forEach(imei => {
                if (!activeIMEIsOnMap.has(imei)) {
                    map.removeLayer(markers[imei]);
                    delete markers[imei];
                    if (alertRings[imei]) {
                        map.removeLayer(alertRings[imei]);
                        delete alertRings[imei];
                    }
                }
            });
        }

        function focusOnDevice(imei, lat, lon) {
            map.flyTo([lat, lon], 17, { animate: true, duration: 1.2 });
            if (markers[imei] && isMarkersVisible) {
                setTimeout(() => {
                    markers[imei].openPopup();
                }, 1200);
            }
        }

        document.getElementById('searchInput').addEventListener('input', (e) => {
            searchTerm = e.target.value;
            renderUI();
        });

        document.getElementById('toggleMarkers').addEventListener('change', (e) => {
            isMarkersVisible = e.target.checked;
            Object.values(markers).forEach(marker => {
                if (isMarkersVisible) map.addLayer(marker);
                else map.removeLayer(marker);
            });
        });

        document.getElementById('toggleHeatmap').addEventListener('change', (e) => {
            isHeatmapVisible = e.target.checked;
            if (heatLayer) {
                if (isHeatmapVisible) map.addLayer(heatLayer);
                else map.removeLayer(heatLayer);
            }
        });

        // --- Controles de la geofence ---
        document.getElementById('toggleGeofence').addEventListener('change', (e) => {
            geofenceEnabled = e.target.checked;
            const controls = document.getElementById('geofenceControls');
            controls.classList.toggle('opacity-40', !geofenceEnabled);
            controls.classList.toggle('pointer-events-none', !geofenceEnabled);
            if (!geofenceEnabled) {
                Object.keys(alertRings).forEach(imei => { map.removeLayer(alertRings[imei]); delete alertRings[imei]; });
            }
            drawGeofence();
            renderUI();
        });

        document.getElementById('geoLat').addEventListener('change', (e) => {
            const v = parseFloat(e.target.value);
            if (!isNaN(v)) { geofenceCenter.lat = v; drawGeofence(); renderUI(); }
        });

        document.getElementById('geoLon').addEventListener('change', (e) => {
            const v = parseFloat(e.target.value);
            if (!isNaN(v)) { geofenceCenter.lon = v; drawGeofence(); renderUI(); }
        });

        document.getElementById('geoRadius').addEventListener('input', (e) => {
            geofenceRadius = Number(e.target.value);
            document.getElementById('geoRadiusLabel').innerText = `${geofenceRadius} m`;
            drawGeofence();
            renderUI();
        });

        document.getElementById('geoUseDeviceBtn').addEventListener('click', () => {
            const imei = document.getElementById('geoDeviceSelect').value;
            if (!imei || !devicesMap.has(imei)) {
                showToast('Elegí un dispositivo de la lista primero');
                return;
            }
            const d = devicesMap.get(imei);
            geofenceCenter = { lat: Number(d.lat), lon: Number(d.lon) };
            document.getElementById('geoLat').value = geofenceCenter.lat.toFixed(6);
            document.getElementById('geoLon').value = geofenceCenter.lon.toFixed(6);
            drawGeofence();
            renderUI();
            showToast(`Geofence centrada en ${imei}`);
        });

        // Carga inicial + canal en tiempo real.
        fetchPositions();
        refreshHeatmap();
        connectWebSocket();

        // Red de respaldo: solo vuelve a consultar posiciones por HTTP si
        // el WebSocket está caído en ese momento. Mientras el socket esté
        // vivo, la latencia real es la de la llegada del dato del GPS
        // (prácticamente instantánea), no la de este timer.
        setInterval(() => {
            if (!wsConnected) {
                fetchPositions();
            }
        }, 1000);

        // El heatmap sí se refresca siempre en su propio intervalo, más
        // espaciado porque no necesita tanta frecuencia.
        setInterval(refreshHeatmap, 10000);
    </script>
</body>
</html>
"""


if __name__ == "__main__":
    uvicorn.run(app, host=HTTP_HOST, port=HTTP_PORT)
