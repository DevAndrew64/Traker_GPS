# Prueba Técnica GPS - MotoSmart

Dejo acá anotado cómo se sube y se corre esto, para no tener que acordarme de memoria cada vez.

## Acceso al servidor

Se entra por SSH:

```
ssh root@24.199.90.9
```

Clave:

```
W/Lca$Xk/.JTYE7Motosmart
```

## Cómo correr el server

Una vez adentro, hay que activar el entorno virtual antes de hacer cualquier cosa:

```
source venv/bin/activate
```

Y ahí sí, se corre normal:

```
python3 server.py
```

Esto deja el servidor TCP escuchando en el puerto 5000 (ahí es donde entran los dispositivos GPS) y el HTTP/WebSocket en el 8000, que es lo que se ve en el navegador entrando a `http://24.199.90.9:8000`.

Si se corta la sesión SSH el proceso se muere con ella, así que para dejarlo corriendo de verdad hay que lanzarlo en background o con algo tipo `nohup`/`screen`/`tmux` (por ahora lo estoy corriendo directo en la sesión, ojo con eso).

## Cómo actualizar el archivo cuando se corrige algo

El flujo que vengo usando es simple: borro el que está y subo el nuevo por scp.

Adentro del servidor, con el proceso detenido (Ctrl+C si está corriendo):

```
rm server.py
exit
```

Y ya desde mi máquina local:

```
scp (ruta del archivo) root@24.199.90.9:/root/
```

Después vuelvo a entrar por SSH, activo el venv de nuevo y corro `python3 server.py` como arriba.

## Cosas a tener en cuenta

- El venv ya tiene las dependencias instaladas (fastapi, uvicorn, supabase, etc), no hace falta reinstalar nada mientras no se agregue una librería nueva. Si se agrega algo, toca `pip install` esa dependencia dentro del venv antes de correr.
- La conexión a Supabase usa el service role key, que está de una vez en el código como fallback si no hay variables de entorno seteadas. Sería bueno en algún momento sacarlo de ahí y rotarlo, pero para la prueba así quedó funcionando.
- El servidor TCP habla el protocolo GT06 (login, heartbeat, ubicación), y también quedó agregado el protocolo 0x22 para el segundo tipo de dispositivo.
- El WebSocket empuja la posición apenas llega del GPS, no depende de ningún timer. El polling por HTTP solo entra como respaldo si el socket se cae, y ahí sí a 1 segundo.
- Si Supabase falla al guardar un dato (por lo que sea, red, rate limit), la conexión TCP con el GPS no se cae, sigue viva esperando la próxima trama.
- El signo de lat/lon quedó forzado según la zona donde opera la flota (Cali: lat positiva, lon negativa), porque el bit de hemisferio que mandan los dispositivos no es confiable entre protocolos. Si el día de mañana hay unidades operando en otra parte del mundo, esto hay que revisarlo.

## Pendiente / punto de mejora

Lo que falta y quedó identificado como mejora a futuro es el geofence: poder marcar una zona en el mapa y que el sistema avise (o marque el dispositivo de otra forma) cuando una unidad sale de esa zona. Ahorita el sistema solo muestra dónde está cada dispositivo en tiempo real, pero no valida contra ningún perímetro. Quedaría para una siguiente iteración.
