# Manual: MCP de SECOP II en ChatGPT

Este manual te lleva de la mano para que tu servidor MCP de SECOP II (el mismo que ya usas en Claude Desktop) también funcione en ChatGPT.

**Diferencia clave:** ChatGPT no puede correr un archivo `.js` desde tu computadora como hace Claude Desktop. Solo se conecta a servidores que tengan una URL pública (https://...). Por eso vamos a "publicar" tu servidor en internet usando un servicio gratuito llamado **Render**, y luego conectamos esa URL a ChatGPT.

Los archivos `server.js` y `package.json` de esta carpeta ya están adaptados y probados — no necesitas tocar el código.

---

## Parte 1: Subir el código a GitHub (sin usar comandos de terminal)

Render necesita el código en un repositorio de GitHub para poder desplegarlo.

### Paso 1.1 — Crear cuenta en GitHub (si no tienes una)
1. Ve a https://github.com/signup
2. Sigue los pasos con tu correo electrónico.

### Paso 1.2 — Crear un repositorio nuevo
1. Ya logueado, ve a https://github.com/new
2. En "Repository name" escribe: `secop-mcp-http`
3. Marca la opción **Private** (privado) si prefieres que no sea público.
4. Haz clic en **Create repository**.

### Paso 1.3 — Subir los archivos (arrastrar y soltar, sin terminal)
1. En la página del repositorio recién creado, busca el enlace que dice **"uploading an existing file"**.
2. Arrastra estos 2 archivos desde tu carpeta a la ventana del navegador:
   - `server.js`
   - `package.json`
3. Escribe un mensaje corto como "primera versión" y haz clic en **Commit changes**.

---

## Parte 2: Desplegar en Render (hosting gratuito)

### Paso 2.1 — Crear cuenta en Render
1. Ve a https://render.com
2. Haz clic en **Get Started** y regístrate (puedes usar tu cuenta de GitHub para entrar más rápido — recomendado).

### Paso 2.2 — Crear el servicio web
1. En el panel de Render, haz clic en **New +** → **Web Service**.
2. Conecta tu cuenta de GitHub si te lo pide, y selecciona el repositorio `secop-mcp-http`.
3. Completa los campos:
   - **Name:** `secop-mcp` (o el nombre que prefieras)
   - **Region:** la más cercana (Oregon o similar)
   - **Branch:** `main`
   - **Runtime:** Node
   - **Build Command:** `npm install`
   - **Start Command:** `node server.js`
   - **Instance Type:** **Free**
4. Haz clic en **Create Web Service**.

### Paso 2.3 — Esperar el despliegue
Render va a instalar las dependencias y arrancar el servidor. Toma 2-5 minutos. Cuando el estado diga **"Live"** (en verde), ya está funcionando.

### Paso 2.4 — Copiar tu URL
Arriba de la página verás una URL como:
```
https://secop-mcp.onrender.com
```
Guarda esa URL — la vas a necesitar en el siguiente paso. Puedes verificar que funciona abriendo en el navegador:
```
https://secop-mcp.onrender.com/health
```
Debe mostrar: `{"status":"ok"}`

**Nota:** en el plan gratuito, el servicio "duerme" después de 15 minutos sin uso. La primera consulta del día puede tardar unos 30-50 segundos en responder mientras Render lo "despierta" — es normal, no es un error.

---

## Parte 3: Conectar el MCP en ChatGPT

### Paso 3.1 — Activar el Modo Desarrollador
1. Abre ChatGPT en el navegador (chatgpt.com).
2. Haz clic en tu foto de perfil (esquina superior derecha) → **Settings**.
3. Ve a **Connectors** (Conectores).
4. Haz clic en **Advanced** (Avanzado), al final de la página.
5. Activa el interruptor de **Developer mode** (Modo desarrollador).
6. Acepta la advertencia sobre conectores personalizados.

### Paso 3.2 — Agregar el conector personalizado
1. Vuelve a la pestaña **Connectors**.
2. Haz clic en **Add custom connector** (Agregar conector personalizado).
3. Completa:
   - **Name:** `SECOP II`
   - **MCP Server URL:** `https://secop-mcp.onrender.com/mcp` (tu URL de Render + `/mcp`)
   - **Authentication:** `No authentication`
4. Guarda.

### Paso 3.3 — Usarlo en una conversación
1. Inicia un chat nuevo.
2. Haz clic en el ícono de herramientas/conectores (el "+" o el ícono junto al campo de texto).
3. Activa **Developer mode** para esa conversación y selecciona el conector **SECOP II**.
4. Escribe tu consulta, por ejemplo:
   > "Busca los 5 contratos de mayor valor en vías dentro de municipios PDET usando SECOP II"
5. ChatGPT te pedirá confirmar el uso de la herramienta la primera vez — acepta.

---

## Resolución de problemas

| Problema | Causa probable | Solución |
|---|---|---|
| Render dice "Build failed" | Falta algún archivo o error de sintaxis | Revisa que subiste `server.js` y `package.json` completos y sin editar |
| La primera consulta tarda mucho | El servicio gratuito estaba dormido | Espera 30-60 seg, es normal en el plan gratuito |
| ChatGPT no encuentra el conector | URL mal escrita | Verifica que termine en `/mcp` (no solo la URL base) |
| Error de "método no permitido" | Falta el `/mcp` al final de la URL | Corrige la URL en la configuración del conector |

---

## Notas importantes

- Este servidor consulta **datos públicos y abiertos** de datos.gov.co — no requiere ni expone ninguna credencial.
- Si en el futuro modificas `server.js`, sube el archivo actualizado a GitHub y Render lo vuelve a desplegar automáticamente.
- El plan gratuito de Render es suficiente para uso personal de consulta ocasional; si necesitas que esté siempre despierto sin demora, existe un plan pago desde ~$7 USD/mes.
