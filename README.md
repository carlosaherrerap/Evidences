# Sistema de Procesamiento de Evidencias - Cobranzas

Aplicación de escritorio para la generación automática de archivos de evidencias de gestión de cobranzas (IVR, SMS, CALL).

## 📋 Características

- **Interfaz moderna y profesional** usando CustomTkinter
- **Procesamiento automático** de múltiples tipos de evidencias
- **Sanitización inteligente** de campos con diferentes variaciones de nombres
- **Log en tiempo real** del proceso de generación
- **Validación de archivos** antes del procesamiento

## 🚀 Instalación

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Ejecutar la aplicación

```bash
python evidencias_app.py
```

## 📖 Uso

### Archivos Necesarios

1. **datos_fuente.xlsx**: Archivo principal con los clientes a procesar
   - Campos requeridos: CUENTA, NOMBRE, DNI, TELEFONO, GESTION EFECTIVA

2. **nuevos_datos.xlsx**: Datos de gestiones efectivas realizadas
   - Campos requeridos: CUENTA, GESTION_EFECTIVA

3. **Audio IVR (.mp3)**: Audio pregrabado único para todas las gestiones IVR

4. **sms.xlsx**: Datos de envíos SMS
   - Campos requeridos: NUMERO DE CREDITO

5. **consolidados.xlsx**: Datos de audios de llamadas
   - Campos requeridos: dni, telefono, ruta, nombre_completo

### Flujo de Trabajo

1. **Sección DATOS BASE**:
   - Seleccionar `datos_fuente.xlsx` (muestra cantidad de clientes)
   - Seleccionar `nuevos_datos.xlsx`

2. **Sección IVR**:
   - Seleccionar audio IVR (.mp3) que se usará para todos los clientes

3. **Sección SMS**:
   - Seleccionar archivo `sms.xlsx`

4. **Sección CALL**:
   - Seleccionar archivo `consolidados.xlsx`

5. **Configuración de Salida**:
   - Seleccionar carpeta donde se guardarán las evidencias
   - Ingresar nombre para la carpeta contenedora

6. **Procesar**:
   - Hacer clic en "PROCESAR EVIDENCIAS"
   - Monitorear el progreso en el log

## 📁 Estructura de Salida

```
[Carpeta Contenedora]/
├── [NOMBRE_CLIENTE_1]_[CUENTA]/
│   ├── [NOMBRE_CLIENTE_1]_ivr.xlsx
│   ├── ivr_[NOMBRE_CLIENTE_1].mp3
│   ├── SMS_[NOMBRE_CLIENTE_1].xlsx
│   ├── [NOMBRE_CLIENTE_1]_gestiones.xlsx
│   └── [NOMBRE_CLIENTE_1]_[CUENTA].mp3
├── [NOMBRE_CLIENTE_2]_[CUENTA]/
│   └── ...
```

### Tipos de Archivos Generados

**IVR** (2 archivos):
- Excel: `[NOMBRE]_ivr.xlsx`
- Audio: `ivr_[NOMBRE].mp3`

**SMS** (1 archivo):
- Excel: `SMS_[NOMBRE].xlsx`

**CALL** (2 archivos):
- Excel: `[NOMBRE]_gestiones.xlsx`
- Audio: `[NOMBRE]_[CUENTA].mp3`

## ⚙️ Características Técnicas

### Sanitización de Campos

La aplicación normaliza automáticamente las variaciones de nombres de campos:

- `CUENTA / cuenta` → `cuenta`
- `NOMBRE / nombres / contacto` → `nombre`
- `DNI / documento` → `dni`
- `TELEFONO / celular` → `telefono`
- `GESTION EFECTIVA / gestión efectiva` → `gestion_efectiva`

### Procesamiento Inteligente

- Solo se crean evidencias que corresponden a cada cliente
- Si un cliente tiene `IVR,SMS` → se crean 3 archivos (2 IVR + 1 SMS)
- Si un cliente tiene `IVR,SMS,GRABACION CALL` → se crean 5 archivos (2 IVR + 1 SMS + 2 CALL)
- Los espacios en blanco adelante y atrás se eliminan automáticamente

### Búsqueda de Audios CALL

Para encontrar los audios de llamadas:
1. Primero busca por DNI en `consolidados.xlsx`
2. Si no encuentra, busca por TELEFONO
3. Construye la ruta: `{ruta}/{nombre_completo}.mp3`

## 🎨 Interfaz

- **Tema oscuro moderno**
- **Indicadores visuales** de archivos seleccionados
- **Contador de clientes** en tiempo real
- **Terminal de logs** con emojis para mejor legibilidad
- **Barra de progreso** textual mostrando cliente actual / total

## ⚠️ Notas Importantes

- El archivo `consolidados.xlsx` NO se sanitiza para preservar las rutas exactas de los audios
- Todos los archivos Excel se generan con codificación correcta usando openpyxl
- El procesamiento se ejecuta en un hilo separado para no bloquear la interfaz
- Los errores se registran en el log pero no detienen el procesamiento completo

## 📝 Requisitos del Sistema

- Python 3.7+
- Windows / macOS / Linux
- Dependencias listadas en `requirements.txt`

## 🐛 Solución de Problemas

**Error: "Audio no encontrado"**
- Verificar que la ruta en `consolidados.xlsx` sea accesible
- Verificar que el archivo tenga extensión `.mp3`

**Error: "Faltan campos"**
- Revisar que los archivos Excel tengan las columnas requeridas
- Los nombres de columnas pueden tener variaciones (se sanitizan automáticamente)

**Error al cargar archivo**
- Verificar que el archivo sea un Excel válido (.xlsx)
- Verificar que no esté abierto en otro programa

## 👥 Soporte

Para reportar problemas o sugerencias, contactar al equipo de desarrollo.
