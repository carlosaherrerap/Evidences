# Guía Rápida de Uso

## Inicio Rápido

### Opción 1: Doble clic en el archivo batch
```
iniciar_app.bat
```

### Opción 2: Desde terminal
```bash
python evidencias_app.py
```

## Preparación de Archivos

### 1. datos_fuente.xlsx
Debe contener las siguientes columnas (pueden tener variaciones):
- CUENTA
- NOMBRE (o nombres, contacto, nombre completo)
- DNI (o documento)
- TELEFONO (o celular, teléfono)
- GESTION EFECTIVA

Ejemplo de GESTION EFECTIVA:
```
IVR,SMS
IVR,SMS
GRABACION CALL,IVR,SMS
IVR,SMS,CALL
```

### 2. nuevos_datos.xlsx
Debe contener:
- CUENTA
- GESTION_EFECTIVA (debe contener IVR o CALL según corresponda)
- Otros campos adicionales se incluirán en el Excel de evidencia

### 3. sms.xlsx
Debe contener:
- NUMERO DE CREDITO (o variaciones del nombre)
- Otros campos que se incluirán en la evidencia SMS

### 4. consolidados.xlsx
Debe contener:
- dni
- telefono
- ruta (ruta base del archivo mp3)
- nombre_completo (nombre del archivo sin extensión)

**IMPORTANTE**: Este archivo NO se sanitiza para preservar las rutas exactas.

### 5. Audio IVR
Un solo archivo .mp3 que se copiará para todos los clientes con gestión IVR.

## Flujo Paso a Paso

1. ✅ Seleccionar datos_fuente.xlsx → Ver cantidad de clientes
2. ✅ Seleccionar nuevos_datos.xlsx
3. ✅ Seleccionar audio IVR (.mp3)
4. ✅ Seleccionar sms.xlsx
5. ✅ Seleccionar consolidados.xlsx
6. ✅ Elegir carpeta de salida
7. ✅ Ingresar nombre de carpeta contenedora
8. ✅ Hacer clic en "PROCESAR EVIDENCIAS"
9. ✅ Esperar a que termine (ver progreso en el log)

## Ejemplo de Resultado

Para el cliente: **GABANCHO CACERES, BANZER** con cuenta **107069101002288680** y gestión efectiva **IVR,SMS,GRABACION CALL**

Se creará la carpeta:
```
GABANCHO CACERES, BANZER_107069101002288680/
├── GABANCHO CACERES, BANZER_ivr.xlsx
├── ivr_GABANCHO CACERES, BANZER.mp3
├── SMS_GABANCHO CACERES, BANZER.xlsx
├── GABANCHO CACERES, BANZER_gestiones.xlsx
└── GABANCHO CACERES, BANZER_107069101002288680.mp3
```

## Validaciones Automáticas

✅ Verifica que todos los archivos estén seleccionados
✅ Valida que existan los campos requeridos
✅ Sanitiza nombres de columnas automáticamente
✅ Elimina espacios en blanco
✅ Solo crea archivos para gestiones que corresponden al cliente

## Solución de Problemas Comunes

### "Faltan campos en el archivo"
- Revisa que el archivo Excel tenga las columnas necesarias
- Los nombres pueden variar (cuenta/CUENTA, nombre/NOMBRE, etc.)

### "Audio no encontrado"
- Para audios CALL, verifica que la ruta en consolidados.xlsx sea correcta
- Verifica que el archivo exista en la ubicación especificada

### "No se encontraron registros"
- Verifica que el número de CUENTA coincida entre archivos
- Revisa que el campo GESTION_EFECTIVA contenga el tipo correcto

## Sistema de Logs

📁 Carpeta creada
✅ Operación exitosa
⚠️ Advertencia
❌ Error
📊 Estadística/resumen
🚀 Inicio de proceso

## Notas Importantes

- El procesamiento puede tomar varios minutos según la cantidad de clientes
- No cerrar la aplicación mientras procesa
- Las evidencias se organizan automáticamente por cliente
- Cada cliente solo tiene las evidencias que le corresponden según su GESTION EFECTIVA
