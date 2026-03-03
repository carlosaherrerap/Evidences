"""
Módulo de procesamiento de datos para evidencias de cobranzas
Maneja la sanitización de campos y generación de archivos de evidencias
"""
import pandas as pd
import numpy as np
import os
import shutil
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.styles import numbers
from typing import Dict, List, Tuple, Optional


class DataProcessor:
    """Procesador de datos para generación de evidencias de gestión"""
    
    def __init__(self, log_callback=None):
        """
        Inicializa el procesador de datos
        
        Args:
            log_callback: Función para enviar mensajes de log a la interfaz
        """
        self.log_callback = log_callback
        
        # Mapeo de nombres de campos para sanitización
        self.field_mappings = {
            'cuenta': ['cuenta', 'CUENTA', 'Cuenta'],
            'nombre': ['nombre', 'NOMBRE', 'nombres', 'NOMBRES', 'contacto', 'CONTACTO', 
                      'nombre completo', 'NOMBRE COMPLETO', 'nombre_completo', 'NOMBRE_COMPLETO'],
            'dni': ['dni', 'DNI', 'documento', 'DOCUMENTO', 'Dni', 'Documento'],
            'gestion_efectiva': ['gestion efectiva', 'GESTION EFECTIVA', 'gestión efectiva', 
                                'GESTIÓN EFECTIVA', 'gestion_efectiva', 'GESTION_EFECTIVA'],
            'telefono': ['telefono', 'TELEFONO', 'teléfono', 'TELÉFONO', 'celular', 
                        'CELULAR', 'Telefono', 'Celular'],
            'tipo_gestion': ['tipo de gestion', 'TIPO DE GESTION', 'tipo_gestion', 'TIPO_GESTION',
                           'tipo de gestión', 'TIPO DE GESTIÓN'],
            'numero_credito': ['numero de credito', 'NUMERO DE CREDITO', 'número de crédito',
                             'NÚMERO DE CRÉDITO', 'numero_credito', 'NUMERO_CREDITO'],
            'ruta': ['ruta', 'RUTA', 'Ruta'],
            'nombre_completo_audio': ['nombre_completo', 'NOMBRE_COMPLETO', 'nombre completo']
        }
    
    def log(self, message: str):
        """Envía un mensaje de log a la interfaz"""
        if self.log_callback:
            self.log_callback(message)
            
    def clean_id(self, value):
        """
        Limpia IDs numéricos (DNI, Teléfono, Cuenta) de forma segura.
        Preserva la precisión total evitando conversiones a float.
        """
        if pd.isna(value) or value == '':
            return ""
        
        # Convertir a string de forma segura
        s_val = str(value).strip()
        
        # Si tiene .0 al final (típico de floats en Excel), lo quitamos SIN usar float()
        if s_val.endswith('.0'):
            s_val = s_val[:-2]
            
        return s_val
    
    def save_excel_formatted(self, df: pd.DataFrame, excel_path: Path):
        """
        Guarda un DataFrame a Excel con formato de texto para campos numéricos
        y sin valores NaN (se muestran como celdas vacías)
        """
        # Crear copia para no modificar el original
        df_formatted = df.copy()
        
        # Reemplazar NaN con cadena vacía
        df_formatted = df_formatted.fillna('')
        
        # Columnas que deben ser texto puro
        numeric_columns = ['cuenta', 'telefono', 'celular', 'dni', 'documento',
                          'numero_credito', 'CUENTA', 'TELEFONO', 'CELULAR', 'DNI',
                          'DOCUMENTO', 'NUMERO DE CREDITO', 'numero de credito']
        
        # Asegurar que todas las columnas ID sean strings limpios
        for col in df_formatted.columns:
            col_lower = col.lower().strip()
            if col in numeric_columns or col_lower in [c.lower() for c in numeric_columns]:
                df_formatted[col] = df_formatted[col].apply(self.clean_id)
        
        # Guardar el archivo Excel
        df_formatted.to_excel(excel_path, index=False, engine='openpyxl')
        
        # Aplicar formato de texto en openpyxl para que Excel lo reconozca como string
        wb = load_workbook(excel_path)
        ws = wb.active
        
        header_row = list(ws.iter_rows(min_row=1, max_row=1, values_only=True))[0]
        numeric_col_indices = []
        for idx, col_name in enumerate(header_row, 1):
            if col_name:
                col_lower = str(col_name).lower().strip()
                if col_name in numeric_columns or col_lower in [c.lower() for c in numeric_columns]:
                    numeric_col_indices.append(idx)
        
        for col_idx in numeric_col_indices:
            for row in range(2, ws.max_row + 1):
                cell = ws.cell(row=row, column=col_idx)
                cell.number_format = numbers.FORMAT_TEXT
        
        wb.save(excel_path)
    
    def sanitize_dataframe(self, df: pd.DataFrame, skip_consolidados: bool = False) -> pd.DataFrame:
        """
        Sanitiza los nombres de columnas de un DataFrame
        
        Args:
            df: DataFrame a sanitizar
            skip_consolidados: Si es True, no sanitiza (para consolidados.xlsx)
            
        Returns:
            DataFrame con columnas sanitizadas
        """
        if skip_consolidados:
            # Solo quitar espacios en blanco de valores, no cambiar nombres de columnas
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            return df
        
        df_copy = df.copy()
        
        # Renombrar columnas según el mapeo
        column_rename = {}
        for standard_name, variations in self.field_mappings.items():
            for col in df_copy.columns:
                if col.strip() in variations:
                    column_rename[col] = standard_name
                    break
        
        df_copy.rename(columns=column_rename, inplace=True)
        
        # Quitar espacios en blanco de los valores y limpiar IDs
        for col in df_copy.columns:
            col_lower = col.lower().strip()
            # Identificar columnas que deben ser tratadas como IDs limpios
            is_id_col = col in ['cuenta', 'dni', 'telefono', 'numero_credito', 'documento', 'celular'] or \
                        col_lower in ['cuenta', 'dni', 'telefono', 'numero_credito', 'documento', 'celular']
            
            if is_id_col:
                df_copy[col] = df_copy[col].apply(self.clean_id)
            elif df_copy[col].dtype == 'object':
                df_copy[col] = df_copy[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        return df_copy
    
    def parse_gestion_efectiva(self, gestion_str: str) -> List[str]:
        """
        Parsea el campo GESTION EFECTIVA separado por comas
        
        Args:
            gestion_str: String con gestiones separadas por coma
            
        Returns:
            Lista de gestiones (IVR, SMS, CALL, GRABACION CALL)
        """
        if pd.isna(gestion_str):
            return []
        
        gestiones = [g.strip().upper() for g in str(gestion_str).split(',')]
        
        # Normalizar GRABACION CALL a CALL
        gestiones = ['CALL' if 'CALL' in g else g for g in gestiones]
        
        return list(set(gestiones))  # Eliminar duplicados
    
    def create_ivr_evidence(self, cliente_data: Dict, nuevos_datos_df: pd.DataFrame, 
                           output_folder: Path, audio_ivr_path: str) -> Tuple[bool, List[str]]:
        """
        Crea archivos de evidencia IVR para un cliente
        
        Returns:
            Tuple (success, files_created)
        """
        files_created = []
        
        try:
            cuenta = cliente_data['cuenta']
            nombre = cliente_data['nombre']
            
            # Copiar audio IVR (SIEMPRE se copia si el cliente tiene gestión IVR)
            audio_filename = f"ivr_{nombre}.mp3"
            audio_path = output_folder / audio_filename
            shutil.copy2(audio_ivr_path, audio_path)
            files_created.append(audio_filename)
            
            # Filtrar en nuevos_datos por CUENTA y GESTION_EFECTIVA = IVR
            ivr_data = nuevos_datos_df[
                (nuevos_datos_df['cuenta'] == cuenta) & 
                (nuevos_datos_df['gestion_efectiva'].str.contains('IVR', na=False))
            ].copy()
            
            if ivr_data.empty:
                self.log(f"  ⚠️ No se encontraron registros IVR en nuevos_datos para {nombre} (audio IVR copiado)")
            else:
                # Agregar columna TIPO DE GESTION
                ivr_data['TIPO DE GESTION'] = 'IVR'
                
                # Crear archivo Excel con formato de texto para campos numéricos
                excel_filename = f"{nombre}_ivr.xlsx"
                excel_path = output_folder / excel_filename
                self.save_excel_formatted(ivr_data, excel_path)
                files_created.append(excel_filename)
            
            return True, files_created
            
        except Exception as e:
            self.log(f"  ❌ Error creando evidencia IVR: {str(e)}")
            return False, files_created
    
    def create_sms_evidence(self, cliente_data: Dict, sms_df: pd.DataFrame, 
                           output_folder: Path) -> Tuple[bool, List[str]]:
        """
        Crea archivo de evidencia SMS para un cliente
        
        Returns:
            Tuple (success, files_created)
        """
        files_created = []
        
        try:
            cuenta = cliente_data['cuenta']
            nombre = cliente_data['nombre']
            
            # Filtrar en sms.xlsx por NUMERO DE CREDITO
            sms_data = sms_df[sms_df['numero_credito'] == cuenta].copy()
            
            if sms_data.empty:
                self.log(f"  ⚠️ No se encontraron registros SMS para {nombre}")
                return False, files_created
            
            # Crear archivo Excel con formato de texto para campos numéricos
            excel_filename = f"SMS_{nombre}.xlsx"
            excel_path = output_folder / excel_filename
            self.save_excel_formatted(sms_data, excel_path)
            files_created.append(excel_filename)
            
            return True, files_created
            
        except Exception as e:
            self.log(f"  ❌ Error creando evidencia SMS: {str(e)}")
            return False, files_created
    
    def create_call_evidence(self, cliente_data: Dict, nuevos_datos_df: pd.DataFrame,
                            consolidados_df: Optional[pd.DataFrame], output_folder: Path) -> Tuple[bool, List[str]]:
        """
        Crea archivos de evidencia CALL para un cliente
        
        Returns:
            Tuple (success, files_created)
        """
        files_created = []
        
        try:
            cuenta = cliente_data['cuenta']
            nombre = cliente_data['nombre']
            dni = cliente_data.get('dni', '')
            telefono = cliente_data.get('telefono', '')
            
            # Filtrar en nuevos_datos por CUENTA y GESTION_EFECTIVA = CALL
            call_data = nuevos_datos_df[
                (nuevos_datos_df['cuenta'] == cuenta) & 
                (nuevos_datos_df['gestion_efectiva'].str.contains('CALL', na=False))
            ].copy()
            
            if call_data.empty:
                self.log(f"  ⚠️ No se encontraron registros CALL en nuevos_datos para {nombre}")
                return False, files_created
            
            # Agregar columna TIPO DE GESTION
            call_data['TIPO DE GESTION'] = 'CALL'
            
            # Crear archivo Excel con formato de texto para campos numéricos
            excel_filename = f"{nombre}_gestiones.xlsx"
            excel_path = output_folder / excel_filename
            self.save_excel_formatted(call_data, excel_path)
            files_created.append(excel_filename)
            
            # Buscar audio y transcripción en consolidados (OPCIONAL - solo si existe consolidados_df)
            if consolidados_df is not None:
                audio_found = False
                audio_row = None
                
                # Primero intentar buscar por DNI
                if dni:
                    dni_clean = self.clean_id(dni)
                    audio_row = consolidados_df[consolidados_df['dni'].apply(self.clean_id) == dni_clean]
                    if not audio_row.empty:
                        audio_found = True
                
                # Si no se encontró por DNI, buscar por teléfono
                if not audio_found and telefono:
                    tel_clean = self.clean_id(telefono)
                    audio_row = consolidados_df[consolidados_df['telefono'].apply(self.clean_id) == tel_clean]
                    if not audio_row.empty:
                        audio_found = True
                
                if audio_found and not audio_row.empty:
                    # Obtener nombre_completo para buscar audio y transcripción
                    ruta = str(audio_row.iloc[0]['ruta'])
                    nombre_completo_audio = str(audio_row.iloc[0]['nombre_completo'])
                    
                    # 1. Copiar audio MP3
                    audio_source_path = f"{ruta}/{nombre_completo_audio}.mp3"
                    
                    if os.path.exists(audio_source_path):
                        audio_filename = f"{nombre}_{cuenta}.mp3"
                        audio_dest_path = output_folder / audio_filename
                        shutil.copy2(audio_source_path, audio_dest_path)
                        files_created.append(audio_filename)
                    else:
                        self.log(f"  ⚠️ Audio no encontrado en: {audio_source_path}")
                    
                    # 2. Buscar y copiar archivo de transcripción TXT
                    transcripcion_base_path = "E:/ProcesoAudios/2025/everyVerse/15-19/evidencias_general"
                    transcripcion_source_path = f"{transcripcion_base_path}/{nombre_completo_audio}.txt"
                    
                    if os.path.exists(transcripcion_source_path):
                        transcripcion_filename = f"{nombre}_{cuenta}.txt"
                        transcripcion_dest_path = output_folder / transcripcion_filename
                        shutil.copy2(transcripcion_source_path, transcripcion_dest_path)
                        files_created.append(transcripcion_filename)
                    else:
                        self.log(f"  ⚠️ Transcripción no encontrada en: {transcripcion_source_path}")
                else:
                    self.log(f"  ⚠️ No se encontró audio CALL para {nombre} (DNI: {self.clean_id(dni)}, TEL: {self.clean_id(telefono)}) - Excel creado")
            else:
                self.log(f"  ℹ️ consolidados.xlsx no proporcionado - Solo Excel CALL creado para {nombre}")
            
            return True, files_created
            
        except Exception as e:
            self.log(f"  ❌ Error creando evidencia CALL: {str(e)}")
            return False, files_created
    
    def process_cliente(self, cliente_row: pd.Series, nuevos_datos_df: pd.DataFrame,
                       sms_df: Optional[pd.DataFrame], consolidados_df: Optional[pd.DataFrame],
                       audio_ivr_path: str, base_output_folder: Path) -> bool:
        """
        Procesa un cliente individual y crea sus archivos de evidencia
        
        Returns:
            True si se procesó exitosamente
        """
        try:
            # Extraer datos del cliente (limpiando IDs inmediatamente)
            cuenta = self.clean_id(cliente_row['cuenta'])
            nombre = str(cliente_row['nombre'])
            dni = self.clean_id(cliente_row.get('dni', ''))
            telefono = self.clean_id(cliente_row.get('telefono', ''))
            gestion_efectiva_str = str(cliente_row['gestion_efectiva'])
            
            # Parsear gestiones efectivas
            gestiones = self.parse_gestion_efectiva(gestion_efectiva_str)
            
            if not gestiones:
                self.log(f"⚠️ Cliente {nombre} no tiene gestiones efectivas")
                return False
            
            # Crear carpeta del cliente
            folder_name = f"{nombre}_{cuenta}"
            cliente_folder = base_output_folder / folder_name
            cliente_folder.mkdir(parents=True, exist_ok=True)
            
            self.log(f"\n📁 Procesando: {folder_name}")
            self.log(f"  Gestiones: {', '.join(gestiones)}")
            
            cliente_data = {
                'cuenta': cuenta,
                'nombre': nombre,
                'dni': dni,
                'telefono': telefono
            }
            
            files_created_total = []
            
            # Procesar IVR
            if 'IVR' in gestiones:
                success, files = self.create_ivr_evidence(
                    cliente_data, nuevos_datos_df, cliente_folder, audio_ivr_path
                )
                if success:
                    files_created_total.extend(files)
                    self.log(f"  ✅ IVR: {', '.join(files)}")
            
            # Procesar SMS
            if 'SMS' in gestiones and sms_df is not None:
                success, files = self.create_sms_evidence(
                    cliente_data, sms_df, cliente_folder
                )
                if success:
                    files_created_total.extend(files)
                    self.log(f"  ✅ SMS: {', '.join(files)}")
            
            # Procesar CALL (consolidados_df es opcional)
            if 'CALL' in gestiones:
                success, files = self.create_call_evidence(
                    cliente_data, nuevos_datos_df, consolidados_df, cliente_folder
                )
                if success:
                    files_created_total.extend(files)
                    self.log(f"  ✅ CALL: {', '.join(files)}")
            
            self.log(f"  📊 Total archivos creados: {len(files_created_total)}")
            
            return True
            
        except Exception as e:
            self.log(f"❌ Error procesando cliente {nombre}: {str(e)}")
            return False
    
    def validate_dataframe_fields(self, df: pd.DataFrame, required_fields: List[str], 
                                  file_name: str) -> Tuple[bool, str]:
        """
        Valida que un DataFrame contenga los campos requeridos
        
        Returns:
            Tuple (valid, error_message)
        """
        missing_fields = []
        for field in required_fields:
            if field not in df.columns:
                missing_fields.append(field)
        
        if missing_fields:
            return False, f"{file_name}: Faltan campos {', '.join(missing_fields)}"
        
        return True, ""
