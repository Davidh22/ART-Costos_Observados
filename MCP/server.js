#!/usr/bin/env node

/**
 * Servidor MCP para SECOP II - versión HTTP (para ChatGPT / Render)
 * Misma lógica de herramientas que la versión de Claude Desktop (stdio),
 * adaptada para exponerse como servidor remoto vía Streamable HTTP.
 */

const express = require('express');
const { Server } = require('@modelcontextprotocol/sdk/server/index.js');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
const {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} = require('@modelcontextprotocol/sdk/types.js');

const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));

// ---------------------------------------------------------------------------
// Configuración de endpoints de SECOP II en datos.gov.co (igual que original)
// ---------------------------------------------------------------------------
const SECOP_ENDPOINTS = {
  contratos: 'https://www.datos.gov.co/resource/jbjy-vk9h.json',
  procesos: 'https://www.datos.gov.co/resource/p6dx-8zbt.json',
  proveedores: 'https://www.datos.gov.co/resource/qmzu-gj57.json',
  proveedores_procesos: 'https://www.datos.gov.co/resource/vnnb-3zbd.json',
  ubicaciones: 'https://www.datos.gov.co/resource/gra4-pcp2.json',
};

async function fetchSecopData(endpoint, params = {}) {
  const url = new URL(endpoint);

  Object.keys(params).forEach((key) => {
    if (params[key] !== undefined && params[key] !== null && params[key] !== '') {
      url.searchParams.append(key, params[key]);
    }
  });

  console.error(`[SECOP-MCP] Petición a: ${url.toString()}`);

  const response = await fetch(url.toString(), {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'SECOP-MCP-Server/1.0',
    },
  });

  console.error(`[SECOP-MCP] Estado de respuesta: ${response.status} ${response.statusText}`);

  if (!response.ok) {
    const errorText = await response.text();
    console.error(`[SECOP-MCP] Error de API: ${errorText}`);
    throw new Error(`Error en API SECOP: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  console.error(`[SECOP-MCP] Éxito: ${Array.isArray(data) ? data.length : 'N/A'} registros recibidos`);
  return data;
}

// ---------------------------------------------------------------------------
// Definición de herramientas (idéntica a la versión de Claude Desktop)
// ---------------------------------------------------------------------------
const TOOLS = [
  {
    name: 'buscar_contratos',
    description: 'Busca contratos electrónicos en SECOP II. Permite filtrar por entidad, proveedor, valor, fecha, etc.',
    inputSchema: {
      type: 'object',
      properties: {
        limite: { type: 'number', description: 'Número máximo de resultados (default: 100, max: 1000)', default: 100 },
        entidad: { type: 'string', description: 'Nombre de la entidad contratante' },
        proveedor: { type: 'string', description: 'Nombre del proveedor o contratista' },
        valor_minimo: { type: 'number', description: 'Valor mínimo del contrato en COP' },
        valor_maximo: { type: 'number', description: 'Valor máximo del contrato en COP' },
        where_clause: { type: 'string', description: 'Cláusula WHERE de SoQL para filtros avanzados' },
        order: { type: 'string', description: 'Campo por el cual ordenar (ej: "valor_del_contrato DESC")' },
      },
    },
  },
  {
    name: 'buscar_procesos',
    description: 'Busca procesos de contratación en SECOP II. Incluye procesos adjudicados y no adjudicados.',
    inputSchema: {
      type: 'object',
      properties: {
        limite: { type: 'number', description: 'Número máximo de resultados (default: 100)', default: 100 },
        estado: { type: 'string', description: 'Estado del proceso (Adjudicado, Desierto, etc)' },
        modalidad: { type: 'string', description: 'Modalidad de contratación (Licitación Pública, Selección Abreviada, etc)' },
        where_clause: { type: 'string', description: 'Cláusula WHERE de SoQL para filtros personalizados' },
      },
    },
  },
  {
    name: 'buscar_proveedores',
    description: 'Busca proveedores registrados en SECOP II',
    inputSchema: {
      type: 'object',
      properties: {
        limite: { type: 'number', description: 'Número máximo de resultados', default: 100 },
        nombre: { type: 'string', description: 'Nombre del proveedor' },
        where_clause: { type: 'string', description: 'Filtro SoQL personalizado' },
      },
    },
  },
  {
    name: 'estadisticas_contratos',
    description: 'Obtiene estadísticas agregadas sobre contratos (suma, promedio, conteo)',
    inputSchema: {
      type: 'object',
      properties: {
        agrupar_por: { type: 'string', description: 'Campo por el cual agrupar (ej: nombre_entidad, proveedor_adjudicado)' },
        where_clause: { type: 'string', description: 'Filtro SoQL para limitar datos' },
        limite: { type: 'number', description: 'Número de grupos a retornar', default: 50 },
      },
      required: ['agrupar_por'],
    },
  },
  {
    name: 'consulta_avanzada',
    description: 'Ejecuta una consulta SoQL personalizada para análisis avanzados',
    inputSchema: {
      type: 'object',
      properties: {
        dataset: { type: 'string', description: 'Dataset a consultar: contratos, procesos, proveedores', enum: ['contratos', 'procesos', 'proveedores'] },
        select: { type: 'string', description: 'Campos a seleccionar (SELECT en SoQL)' },
        where: { type: 'string', description: 'Condiciones de filtrado (WHERE en SoQL)' },
        group: { type: 'string', description: 'Campos para agrupar (GROUP BY en SoQL)' },
        order: { type: 'string', description: 'Ordenamiento (ORDER BY en SoQL)' },
        limit: { type: 'number', description: 'Límite de resultados', default: 100 },
      },
      required: ['dataset'],
    },
  },
];

// ---------------------------------------------------------------------------
// Ejecución de herramientas (misma lógica que la versión original)
// ---------------------------------------------------------------------------
async function callTool(name, args) {
  switch (name) {
    case 'buscar_contratos': {
      const params = { $limit: args.limite || 100 };
      const where = [];
      if (args.entidad) where.push(`nombre_entidad LIKE '%${args.entidad}%'`);
      if (args.proveedor) where.push(`proveedor_adjudicado LIKE '%${args.proveedor}%'`);
      if (args.valor_minimo) where.push(`valor_del_contrato >= ${args.valor_minimo}`);
      if (args.valor_maximo) where.push(`valor_del_contrato <= ${args.valor_maximo}`);
      if (args.where_clause) where.push(args.where_clause);
      if (where.length) params.$where = where.join(' AND ');
      if (args.order) params.$order = args.order;
      return fetchSecopData(SECOP_ENDPOINTS.contratos, params);
    }
    case 'buscar_procesos': {
      const params = { $limit: args.limite || 100 };
      const where = [];
      if (args.estado) where.push(`estado_proceso = '${args.estado}'`);
      if (args.modalidad) where.push(`modalidad_de_contratacion = '${args.modalidad}'`);
      if (args.where_clause) where.push(args.where_clause);
      if (where.length) params.$where = where.join(' AND ');
      return fetchSecopData(SECOP_ENDPOINTS.procesos, params);
    }
    case 'buscar_proveedores': {
      const params = { $limit: args.limite || 100 };
      if (args.nombre) params.$where = `nombre LIKE '%${args.nombre}%'`;
      if (args.where_clause) params.$where = args.where_clause;
      return fetchSecopData(SECOP_ENDPOINTS.proveedores, params);
    }
    case 'estadisticas_contratos': {
      const params = {
        $select: `${args.agrupar_por}, SUM(valor_del_contrato) as valor_total, AVG(valor_del_contrato) as valor_promedio, COUNT(*) as cantidad`,
        $group: args.agrupar_por,
        $order: 'valor_total DESC',
        $limit: args.limite || 50,
      };
      if (args.where_clause) params.$where = args.where_clause;
      return fetchSecopData(SECOP_ENDPOINTS.contratos, params);
    }
    case 'consulta_avanzada': {
      const endpoint = SECOP_ENDPOINTS[args.dataset];
      if (!endpoint) throw new Error(`Dataset inválido: ${args.dataset}`);
      const params = {};
      if (args.select) params.$select = args.select;
      if (args.where) params.$where = args.where;
      if (args.group) params.$group = args.group;
      if (args.order) params.$order = args.order;
      if (args.limit) params.$limit = args.limit;
      return fetchSecopData(endpoint, params);
    }
    default:
      throw new Error(`Herramienta desconocida: ${name}`);
  }
}

// ---------------------------------------------------------------------------
// Construcción de una instancia del servidor MCP (una por request, modo stateless)
// ---------------------------------------------------------------------------
function buildServer() {
  const server = new Server(
    { name: 'secop-mcp-server', version: '2.0.0-http' },
    { capabilities: { tools: {} } }
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    console.error(`[SECOP-MCP] Ejecutando herramienta: ${name}`);
    try {
      const data = await callTool(name, args || {});
      return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
    } catch (error) {
      console.error(`[SECOP-MCP] Error al ejecutar ${name}:`, error);
      return {
        content: [{ type: 'text', text: `Error: ${error.message}` }],
        isError: true,
      };
    }
  });

  return server;
}

// ---------------------------------------------------------------------------
// Servidor HTTP (Express) - endpoint /mcp que ChatGPT / Claude usan
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json());

app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'secop-mcp-server', mcp_endpoint: '/mcp' });
});

// Endpoint de salud para que Render sepa que el servicio está vivo
app.get('/health', (req, res) => res.json({ status: 'ok' }));

app.post('/mcp', async (req, res) => {
  try {
    const server = buildServer();
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    res.on('close', () => {
      transport.close();
      server.close();
    });
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error('[SECOP-MCP] Error en /mcp:', error);
    if (!res.headersSent) {
      res.status(500).json({ error: 'internal_error', message: error.message });
    }
  }
});

// GET/DELETE en /mcp no se usan en modo stateless, pero respondemos algo válido
app.get('/mcp', (req, res) => {
  res.status(405).json({ error: 'method_not_allowed', message: 'Use POST /mcp' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.error(`[SECOP-MCP] Servidor HTTP escuchando en el puerto ${PORT}`);
});
