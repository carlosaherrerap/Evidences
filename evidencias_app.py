"""
Aplicación GUI para procesamiento de evidencias de cobranzas
Interfaz moderna usando customtkinter
"""
import customtkinter as ctk
from tkinter import filedialog, messagebox
import pandas as pd
import os
import threading
from pathlib import Path
from data_processor import DataProcessor


class EvidenciasApp(ctk.CTk):
    """Aplicación principal para procesamiento de evidencias"""
    
    def __init__(self):
        super().__init__()
        
        # Configuración de la ventana
        self.title("Sistema de Procesamiento de Evidencias - Cobranzas")
        self.geometry("1000x800")
        
        # Configurar tema
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        # Variables para almacenar rutas de archivos
        self.datos_fuente_path = None
        self.nuevos_datos_path = None
        self.audio_ivr_path = None
        self.sms_path = None
        self.consolidados_path = None
        self.output_folder_path = None
        
        # DataFrames cargados
        self.datos_fuente_df = None
        self.nuevos_datos_df = None
        self.sms_df = None
        self.consolidados_df = None
        
        # Procesador de datos
        self.processor = DataProcessor(log_callback=self.log_message)
        
        # Crear interfaz
        self.create_ui()
    
    def create_ui(self):
        """Crea la interfaz de usuario"""
        
        # Contenedor principal con scroll
        self.main_container = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.main_container.pack(fill="both", expand=True, padx=20, pady=20)
        
        # ===== TÍTULO =====
        title_frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        title_frame.pack(fill="x", pady=(0, 20))
        
        title_label = ctk.CTkLabel(
            title_frame,
            text="📋 EVIDENCIAS - SISTEMA DE PROCESAMIENTO",
            font=ctk.CTkFont(size=28, weight="bold")
        )
        title_label.pack()
        
        subtitle_label = ctk.CTkLabel(
            title_frame,
            text="Generación automática de evidencias IVR, SMS y CALL",
            font=ctk.CTkFont(size=14),
            text_color="gray"
        )
        subtitle_label.pack()
        
        # ===== SECCIÓN: DATOS BASE =====
        self.create_section_header("📁 DATOS BASE")
        
        datos_frame = ctk.CTkFrame(self.main_container)
        datos_frame.pack(fill="x", pady=(0, 15))
        
        # datos_fuente.xlsx
        self.create_file_selector(
            datos_frame,
            "datos_fuente.xlsx:",
            "datos_fuente",
            self.on_datos_fuente_selected,
            row=0
        )
        
        # Label para mostrar cantidad de clientes
        self.clientes_label = ctk.CTkLabel(
            datos_frame,
            text="",
            font=ctk.CTkFont(size=12),
            text_color="#4CAF50"
        )
        self.clientes_label.grid(row=1, column=0, columnspan=3, padx=20, pady=(5, 10), sticky="w")
        
        # nuevos_datos.xlsx
        self.create_file_selector(
            datos_frame,
            "nuevos_datos.xlsx:",
            "nuevos_datos",
            self.on_nuevos_datos_selected,
            row=2
        )
        
        # ===== SECCIÓN: IVR =====
        self.create_section_header("🎤 IVR")
        
        ivr_frame = ctk.CTkFrame(self.main_container)
        ivr_frame.pack(fill="x", pady=(0, 15))
        
        self.create_file_selector(
            ivr_frame,
            "Seleccionar audio IVR (.mp3):",
            "audio_ivr",
            self.on_audio_ivr_selected,
            row=0,
            file_types=[("Audio MP3", "*.mp3")]
        )
        
        # ===== SECCIÓN: SMS =====
        self.create_section_header("📱 SMS")
        
        sms_frame = ctk.CTkFrame(self.main_container)
        sms_frame.pack(fill="x", pady=(0, 15))
        
        self.create_file_selector(
            sms_frame,
            "Seleccionar archivo sms.xlsx:",
            "sms",
            self.on_sms_selected,
            row=0
        )
        
        # ===== SECCIÓN: CALL =====
        self.create_section_header("📞 CALL")
        
        call_frame = ctk.CTkFrame(self.main_container)
        call_frame.pack(fill="x", pady=(0, 15))
        
        self.create_file_selector(
            call_frame,
            "Seleccionar archivo consolidados.xlsx:",
            "consolidados",
            self.on_consolidados_selected,
            row=0
        )
        
        # ===== SECCIÓN: CONFIGURACIÓN DE SALIDA =====
        self.create_section_header("💾 CONFIGURACIÓN DE SALIDA")
        
        output_frame = ctk.CTkFrame(self.main_container)
        output_frame.pack(fill="x", pady=(0, 15))
        
        # Selector de carpeta de salida
        folder_label = ctk.CTkLabel(
            output_frame,
            text="Carpeta de salida:",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        folder_label.grid(row=0, column=0, padx=20, pady=10, sticky="w")
        
        self.output_folder_entry = ctk.CTkEntry(
            output_frame,
            placeholder_text="Ninguna carpeta seleccionada",
            width=500,
            state="readonly"
        )
        self.output_folder_entry.grid(row=0, column=1, padx=(10, 10), pady=10, sticky="ew")
        
        self.output_folder_btn = ctk.CTkButton(
            output_frame,
            text="Seleccionar carpeta",
            command=self.select_output_folder,
            width=150
        )
        self.output_folder_btn.grid(row=0, column=2, padx=(0, 20), pady=10)
        
        # Nombre de carpeta principal
        name_label = ctk.CTkLabel(
            output_frame,
            text="Nombre de carpeta contenedora:",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        name_label.grid(row=1, column=0, padx=20, pady=10, sticky="w")
        
        self.folder_name_entry = ctk.CTkEntry(
            output_frame,
            placeholder_text="Ej: Evidencias_2024",
            width=500
        )
        self.folder_name_entry.grid(row=1, column=1, padx=(10, 10), pady=10, sticky="ew")
        
        output_frame.columnconfigure(1, weight=1)
        
        # ===== BOTÓN PROCESAR =====
        process_frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        process_frame.pack(fill="x", pady=(15, 10))
        
        self.process_btn = ctk.CTkButton(
            process_frame,
            text="🚀 PROCESAR EVIDENCIAS",
            command=self.start_processing,
            height=50,
            font=ctk.CTkFont(size=16, weight="bold"),
            fg_color="#4CAF50",
            hover_color="#45a049"
        )
        self.process_btn.pack(pady=10)
        
        # ===== TERMINAL DE LOGS =====
        self.create_section_header("📊 LOG DE PROCESAMIENTO")
        
        log_frame = ctk.CTkFrame(self.main_container)
        log_frame.pack(fill="both", expand=True, pady=(0, 10))
        
        self.log_text = ctk.CTkTextbox(
            log_frame,
            height=250,
            font=ctk.CTkFont(family="Consolas", size=11),
            wrap="word"
        )
        self.log_text.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Mensaje inicial
        self.log_message("💡 Sistema iniciado. Por favor, seleccione los archivos necesarios.")
        self.log_message("=" * 80)
    
    def create_section_header(self, text: str):
        """Crea un encabezado de sección"""
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", pady=(15, 5))
        
        label = ctk.CTkLabel(
            frame,
            text=text,
            font=ctk.CTkFont(size=16, weight="bold"),
            anchor="w"
        )
        label.pack(side="left", padx=5)
        
        # Línea divisoria
        separator = ctk.CTkFrame(frame, height=2, fg_color="gray30")
        separator.pack(side="left", fill="x", expand=True, padx=(10, 0))
    
    def create_file_selector(self, parent, label_text: str, var_name: str, 
                            callback, row: int, file_types=None):
        """Crea un selector de archivo"""
        if file_types is None:
            file_types = [("Excel files", "*.xlsx"), ("All files", "*.*")]
        
        label = ctk.CTkLabel(
            parent,
            text=label_text,
            font=ctk.CTkFont(size=13, weight="bold")
        )
        label.grid(row=row, column=0, padx=20, pady=10, sticky="w")
        
        entry = ctk.CTkEntry(
            parent,
            placeholder_text="Ningún archivo seleccionado",
            width=500,
            state="readonly"
        )
        entry.grid(row=row, column=1, padx=(10, 10), pady=10, sticky="ew")
        
        # Guardar referencia al entry
        setattr(self, f"{var_name}_entry", entry)
        
        btn = ctk.CTkButton(
            parent,
            text="Seleccionar",
            command=lambda: self.select_file(var_name, callback, file_types),
            width=150
        )
        btn.grid(row=row, column=2, padx=(0, 20), pady=10)
        
        # Guardar referencia al botón
        setattr(self, f"{var_name}_btn", btn)
        
        parent.columnconfigure(1, weight=1)
    
    def select_file(self, var_name: str, callback, file_types):
        """Abre diálogo para seleccionar archivo"""
        filename = filedialog.askopenfilename(
            title=f"Seleccionar archivo",
            filetypes=file_types
        )
        
        if filename:
            # Actualizar entry
            entry = getattr(self, f"{var_name}_entry")
            entry.configure(state="normal")
            entry.delete(0, "end")
            entry.insert(0, os.path.basename(filename))
            entry.configure(state="readonly")
            
            # Actualizar botón
            btn = getattr(self, f"{var_name}_btn")
            btn.configure(text="✓ Seleccionado", fg_color="#4CAF50")
            
            # Llamar al callback
            if callback:
                callback(filename)
    
    def select_output_folder(self):
        """Selecciona carpeta de salida"""
        folder = filedialog.askdirectory(title="Seleccionar carpeta de salida")
        
        if folder:
            self.output_folder_path = folder
            self.output_folder_entry.configure(state="normal")
            self.output_folder_entry.delete(0, "end")
            self.output_folder_entry.insert(0, folder)
            self.output_folder_entry.configure(state="readonly")
            
            self.output_folder_btn.configure(text="✓ Seleccionada", fg_color="#4CAF50")
    
    def on_datos_fuente_selected(self, filepath: str):
        """Callback cuando se selecciona datos_fuente.xlsx"""
        try:
            self.datos_fuente_path = filepath
            df = pd.read_excel(filepath, dtype=str)
            self.datos_fuente_df = self.processor.sanitize_dataframe(df)
            
            num_clientes = len(self.datos_fuente_df)
            self.clientes_label.configure(
                text=f"✅ {num_clientes} clientes encontrados | {num_clientes} carpetas a crear"
            )
            
            self.log_message(f"✅ Archivo datos_fuente.xlsx cargado: {num_clientes} clientes")
            
            # Validar campos requeridos
            required = ['cuenta', 'nombre', 'gestion_efectiva']
            valid, error = self.processor.validate_dataframe_fields(
                self.datos_fuente_df, required, "datos_fuente.xlsx"
            )
            if not valid:
                self.log_message(f"⚠️ {error}")
                
        except Exception as e:
            self.log_message(f"❌ Error cargando datos_fuente.xlsx: {str(e)}")
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{str(e)}")
    
    def on_nuevos_datos_selected(self, filepath: str):
        """Callback cuando se selecciona nuevos_datos.xlsx"""
        try:
            self.nuevos_datos_path = filepath
            df = pd.read_excel(filepath, dtype=str)
            self.nuevos_datos_df = self.processor.sanitize_dataframe(df)
            
            self.log_message(f"✅ Archivo nuevos_datos.xlsx cargado: {len(self.nuevos_datos_df)} registros")
            
            # Validar campos requeridos
            required = ['cuenta', 'gestion_efectiva']
            valid, error = self.processor.validate_dataframe_fields(
                self.nuevos_datos_df, required, "nuevos_datos.xlsx"
            )
            if not valid:
                self.log_message(f"⚠️ {error}")
                
        except Exception as e:
            self.log_message(f"❌ Error cargando nuevos_datos.xlsx: {str(e)}")
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{str(e)}")
    
    def on_audio_ivr_selected(self, filepath: str):
        """Callback cuando se selecciona audio IVR"""
        self.audio_ivr_path = filepath
        self.log_message(f"✅ Audio IVR seleccionado: {os.path.basename(filepath)}")
    
    def on_sms_selected(self, filepath: str):
        """Callback cuando se selecciona sms.xlsx"""
        try:
            self.sms_path = filepath
            df = pd.read_excel(filepath, dtype=str)
            self.sms_df = self.processor.sanitize_dataframe(df)
            
            self.log_message(f"✅ Archivo sms.xlsx cargado: {len(self.sms_df)} registros")
            
            # Validar campo requerido
            required = ['numero_credito']
            valid, error = self.processor.validate_dataframe_fields(
                self.sms_df, required, "sms.xlsx"
            )
            if not valid:
                self.log_message(f"⚠️ {error}")
                
        except Exception as e:
            self.log_message(f"❌ Error cargando sms.xlsx: {str(e)}")
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{str(e)}")
    
    def on_consolidados_selected(self, filepath: str):
        """Callback cuando se selecciona consolidados.xlsx"""
        try:
            self.consolidados_path = filepath
            df = pd.read_excel(filepath, dtype=str)
            # No sanitizar consolidados, mantener nombres originales para la ruta
            self.consolidados_df = df
            
            # Solo quitar espacios en blanco
            for col in self.consolidados_df.columns:
                if self.consolidados_df[col].dtype == 'object':
                    self.consolidados_df[col] = self.consolidados_df[col].apply(
                        lambda x: x.strip() if isinstance(x, str) else x
                    )
            
            self.log_message(f"✅ Archivo consolidados.xlsx cargado: {len(self.consolidados_df)} registros")
            
            # Validar campos requeridos (usando nombres originales)
            required = ['dni', 'telefono', 'ruta', 'nombre_completo']
            missing = [f for f in required if f not in self.consolidados_df.columns]
            if missing:
                self.log_message(f"⚠️ consolidados.xlsx: Faltan campos {', '.join(missing)}")
                
        except Exception as e:
            self.log_message(f"❌ Error cargando consolidados.xlsx: {str(e)}")
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{str(e)}")
    
    def log_message(self, message: str):
        """Agrega mensaje al log"""
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.update_idletasks()
    
    def validate_inputs(self) -> bool:
        """Valida que todos los archivos necesarios estén seleccionados"""
        errors = []
        
        if not self.datos_fuente_path:
            errors.append("• datos_fuente.xlsx no seleccionado")
        
        if not self.nuevos_datos_path:
            errors.append("• nuevos_datos.xlsx no seleccionado")
        
        if not self.audio_ivr_path:
            errors.append("• Audio IVR no seleccionado")
        
        if not self.output_folder_path:
            errors.append("• Carpeta de salida no seleccionada")
        
        if not self.folder_name_entry.get().strip():
            errors.append("• Nombre de carpeta contenedora vacío")
        
        if errors:
            error_msg = "Por favor, complete los siguientes campos:\n\n" + "\n".join(errors)
            messagebox.showwarning("Campos incompletos", error_msg)
            self.log_message("⚠️ Validación fallida: campos incompletos")
            return False
        
        return True
    
    def start_processing(self):
        """Inicia el procesamiento en un hilo separado"""
        if not self.validate_inputs():
            return
        
        # Deshabilitar botón de procesamiento
        self.process_btn.configure(state="disabled", text="⏳ Procesando...")
        
        # Limpiar log anterior
        self.log_text.delete("1.0", "end")
        
        # Ejecutar en hilo separado para no bloquear la UI
        thread = threading.Thread(target=self.process_evidencias)
        thread.daemon = True
        thread.start()
    
    def process_evidencias(self):
        """Procesa todas las evidencias"""
        try:
            self.log_message("=" * 80)
            self.log_message("🚀 INICIANDO PROCESAMIENTO DE EVIDENCIAS")
            self.log_message("=" * 80)
            
            # Crear carpeta contenedora
            folder_name = self.folder_name_entry.get().strip()
            base_output = Path(self.output_folder_path) / folder_name
            base_output.mkdir(parents=True, exist_ok=True)
            os.utime(base_output, None) # Asegurar fecha de hoy en la carpeta principal
            
            self.log_message(f"\n📁 Carpeta de salida: {base_output}")
            
            total_clientes = len(self.datos_fuente_df)
            self.log_message(f"📊 Total de clientes a procesar: {total_clientes}\n")
            
            # Procesar cada cliente
            success_count = 0
            for idx, (_, cliente_row) in enumerate(self.datos_fuente_df.iterrows(), 1):
                self.log_message(f"\n[{idx}/{total_clientes}] {'=' * 60}")
                
                success = self.processor.process_cliente(
                    cliente_row,
                    self.nuevos_datos_df,
                    self.sms_df,
                    self.consolidados_df,
                    self.audio_ivr_path,
                    base_output
                )
                
                if success:
                    success_count += 1
            
            # Resumen final
            self.log_message("\n" + "=" * 80)
            self.log_message("✅ PROCESAMIENTO COMPLETADO")
            self.log_message("=" * 80)
            self.log_message(f"📊 Clientes procesados exitosamente: {success_count}/{total_clientes}")
            self.log_message(f"📁 Carpetas creadas en: {base_output}")
            self.log_message("=" * 80)
            
            # Mostrar mensaje de éxito
            self.after(0, lambda: messagebox.showinfo(
                "Procesamiento completado",
                f"✅ Se procesaron {success_count} de {total_clientes} clientes exitosamente.\n\n"
                f"Las evidencias se guardaron en:\n{base_output}"
            ))
            
        except Exception as e:
            error_msg = f"❌ Error durante el procesamiento: {str(e)}"
            self.log_message(f"\n{error_msg}")
            self.after(0, lambda: messagebox.showerror("Error", error_msg))
        
        finally:
            # Rehabilitar botón
            self.after(0, lambda: self.process_btn.configure(
                state="normal",
                text="🚀 PROCESAR EVIDENCIAS"
            ))


def main():
    """Función principal"""
    app = EvidenciasApp()
    app.mainloop()


if __name__ == "__main__":
    main()
