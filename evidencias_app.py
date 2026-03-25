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
from data_processor import ProcesadorDatos


class AppEvidencias(ctk.CTk):
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
        self.ruta_datos_fuente = None
        self.ruta_nuevos_datos = None
        self.ruta_audio_ivr = None
        self.ruta_sms = None
        self.ruta_consolidados = None
        self.ruta_carpeta_salida = None
        
        # DataFrames cargados
        self.df_datos_fuente = None
        self.df_nuevos_datos = None
        self.df_sms = None
        self.df_consolidados = None
        
        # Procesador de datos
        self.procesador = ProcesadorDatos(funcion_log=self.mensajear_log)
        
        # Crear interfaz
        self.crear_interfaz()
    
    def crear_interfaz(self):
        """Crea la interfaz de usuario"""
        
        # Contenedor principal con scroll
        self.contenedor_principal = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.contenedor_principal.pack(fill="both", expand=True, padx=20, pady=20)
        
        # ===== TÍTULO =====
        titulo_frame = ctk.CTkFrame(self.contenedor_principal, fg_color="transparent")
        titulo_frame.pack(fill="x", pady=(0, 20))
        
        titulo_label = ctk.CTkLabel(
            titulo_frame,
            text="📋 EVIDENCIAS - SISTEMA DE PROCESAMIENTO",
            font=ctk.CTkFont(size=28, weight="bold")
        )
        titulo_label.pack()
        
        subtitulo_label = ctk.CTkLabel(
            titulo_frame,
            text="Generación automática de evidencias IVR, SMS y CALL",
            font=ctk.CTkFont(size=14),
            text_color="gray"
        )
        subtitulo_label.pack()
        
        # ===== SECCIÓN: DATOS BASE =====
        self.crear_encabezado_seccion("📁 DATOS BASE")
        
        datos_frame = ctk.CTkFrame(self.contenedor_principal)
        datos_frame.pack(fill="x", pady=(0, 15))
        
        # datos_fuente.xlsx
        self.crear_selector_archivo(
            datos_frame,
            "datos_fuente.xlsx:",
            "datos_fuente",
            self.al_seleccionar_datos_fuente,
            fila=0
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
        self.crear_selector_archivo(
            datos_frame,
            "nuevos_datos.xlsx:",
            "nuevos_datos",
            self.al_seleccionar_nuevos_datos,
            fila=2
        )
        
        # ===== SECCIÓN: IVR =====
        self.crear_encabezado_seccion("🎤 IVR")
        
        ivr_frame = ctk.CTkFrame(self.contenedor_principal)
        ivr_frame.pack(fill="x", pady=(0, 15))
        
        self.crear_selector_archivo(
            ivr_frame,
            "Seleccionar audio IVR (.mp3):",
            "audio_ivr",
            self.al_seleccionar_audio_ivr,
            fila=0,
            tipos_archivo=[("Audio MP3", "*.mp3")]
        )
        
        # ===== SECCIÓN: SMS =====
        self.crear_encabezado_seccion("📱 SMS")
        
        sms_frame = ctk.CTkFrame(self.contenedor_principal)
        sms_frame.pack(fill="x", pady=(0, 15))
        
        self.crear_selector_archivo(
            sms_frame,
            "Seleccionar archivo sms.xlsx:",
            "sms",
            self.al_seleccionar_sms,
            fila=0
        )
        
        # ===== SECCIÓN: CALL =====
        self.crear_encabezado_seccion("📞 CALL")
        
        call_frame = ctk.CTkFrame(self.contenedor_principal)
        call_frame.pack(fill="x", pady=(0, 15))
        
        self.crear_selector_archivo(
            call_frame,
            "Seleccionar archivo consolidados.xlsx:",
            "consolidados",
            self.al_seleccionar_consolidados,
            fila=0
        )
        
        # ===== SECCIÓN: CONFIGURACIÓN DE SALIDA =====
        self.crear_encabezado_seccion("💾 CONFIGURACIÓN DE SALIDA")
        
        salida_frame = ctk.CTkFrame(self.contenedor_principal)
        salida_frame.pack(fill="x", pady=(0, 15))
        
        # Selector de carpeta de salida
        etiqueta_carpeta = ctk.CTkLabel(
            salida_frame,
            text="Carpeta de salida:",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        etiqueta_carpeta.grid(row=0, column=0, padx=20, pady=10, sticky="w")
        
        self.entrada_carpeta_salida = ctk.CTkEntry(
            salida_frame,
            placeholder_text="Ninguna carpeta seleccionada",
            width=500,
            state="readonly"
        )
        self.entrada_carpeta_salida.grid(row=0, column=1, padx=(10, 10), pady=10, sticky="ew")
        
        self.boton_carpeta_salida = ctk.CTkButton(
            salida_frame,
            text="Seleccionar carpeta",
            command=self.seleccionar_carpeta_salida,
            width=150
        )
        self.boton_carpeta_salida.grid(row=0, column=2, padx=(0, 20), pady=10)
        
        # Nombre de carpeta principal
        etiqueta_nombre = ctk.CTkLabel(
            salida_frame,
            text="Nombre de carpeta contenedora:",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        etiqueta_nombre.grid(row=1, column=0, padx=20, pady=10, sticky="w")
        
        self.entrada_nombre_carpeta = ctk.CTkEntry(
            salida_frame,
            placeholder_text="Ej: Evidencias_2024",
            width=500
        )
        self.entrada_nombre_carpeta.grid(row=1, column=1, padx=(10, 10), pady=10, sticky="ew")
        
        salida_frame.columnconfigure(1, weight=1)
        
        # ===== BOTÓN PROCESAR =====
        procesar_frame = ctk.CTkFrame(self.contenedor_principal, fg_color="transparent")
        procesar_frame.pack(fill="x", pady=(15, 10))
        
        self.boton_procesar = ctk.CTkButton(
            procesar_frame,
            text="🚀 PROCESAR EVIDENCIAS",
            command=self.iniciar_procesamiento,
            height=50,
            font=ctk.CTkFont(size=16, weight="bold"),
            fg_color="#4CAF50",
            hover_color="#45a049"
        )
        self.boton_procesar.pack(pady=10)
        
        # ===== TERMINAL DE LOGS =====
        self.crear_encabezado_seccion("📊 LOG DE PROCESAMIENTO")
        
        log_frame = ctk.CTkFrame(self.contenedor_principal)
        log_frame.pack(fill="both", expand=True, pady=(0, 10))
        
        self.texto_log = ctk.CTkTextbox(
            log_frame,
            height=250,
            font=ctk.CTkFont(family="Consolas", size=11),
            wrap="word"
        )
        self.texto_log.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Mensaje inicial
        self.mensajear_log("💡 Sistema iniciado. Por favor, seleccione los archivos necesarios.")
        self.mensajear_log("=" * 80)
    
    def crear_encabezado_seccion(self, texto: str):
        """Crea un encabezado de sección"""
        frame = ctk.CTkFrame(self.contenedor_principal, fg_color="transparent")
        frame.pack(fill="x", pady=(15, 5))
        
        etiqueta = ctk.CTkLabel(
            frame,
            text=texto,
            font=ctk.CTkFont(size=16, weight="bold"),
            anchor="w"
        )
        etiqueta.pack(side="left", padx=5)
        
        # Línea divisoria
        separador = ctk.CTkFrame(frame, height=2, fg_color="gray30")
        separador.pack(side="left", fill="x", expand=True, padx=(10, 0))
    
    def crear_selector_archivo(self, padre, texto_etiqueta: str, nombre_variable: str, 
                            callback_seleccion, fila: int, tipos_archivo=None):
        """Crea un selector de archivo"""
        if tipos_archivo is None:
            tipos_archivo = [("Archivos de Excel", "*.xlsx"), ("Todos los archivos", "*.*")]
        
        etiqueta = ctk.CTkLabel(
            padre,
            text=texto_etiqueta,
            font=ctk.CTkFont(size=13, weight="bold")
        )
        etiqueta.grid(row=fila, column=0, padx=20, pady=10, sticky="w")
        
        entrada = ctk.CTkEntry(
            padre,
            placeholder_text="Ningún archivo seleccionado",
            width=500,
            state="readonly"
        )
        entrada.grid(row=fila, column=1, padx=(10, 10), pady=10, sticky="ew")
        
        # Guardar referencia a la entrada
        setattr(self, f"entrada_{nombre_variable}", entrada)
        
        boton = ctk.CTkButton(
            padre,
            text="Seleccionar",
            command=lambda: self.seleccionar_archivo(nombre_variable, callback_seleccion, tipos_archivo),
            width=150
        )
        boton.grid(row=fila, column=2, padx=(0, 20), pady=10)
        
        # Guardar referencia al botón
        setattr(self, f"boton_{nombre_variable}", boton)
        
        padre.columnconfigure(1, weight=1)
    
    def seleccionar_archivo(self, nombre_variable: str, callback_seleccion, tipos_archivo):
        """Abre diálogo para seleccionar archivo"""
        nombre_archivo = filedialog.askopenfilename(
            title=f"Seleccionar archivo",
            filetypes=tipos_archivo
        )
        
        if nombre_archivo:
            # Actualizar entrada
            entrada = getattr(self, f"entrada_{nombre_variable}")
            entrada.configure(state="normal")
            entrada.delete(0, "end")
            entrada.insert(0, os.path.basename(nombre_archivo))
            entrada.configure(state="readonly")
            
            # Actualizar botón
            boton = getattr(self, f"boton_{nombre_variable}")
            boton.configure(text="✓ Seleccionado", fg_color="#4CAF50")
            
            # Llamar al callback
            if callback_seleccion:
                callback_seleccion(nombre_archivo)
    
    def seleccionar_carpeta_salida(self):
        """Selecciona carpeta de salida"""
        carpeta = filedialog.askdirectory(title="Seleccionar carpeta de salida")
        
        if carpeta:
            self.ruta_carpeta_salida = carpeta
            self.entrada_carpeta_salida.configure(state="normal")
            self.entrada_carpeta_salida.delete(0, "end")
            self.entrada_carpeta_salida.insert(0, carpeta)
            self.entrada_carpeta_salida.configure(state="readonly")
            
            self.boton_carpeta_salida.configure(text="✓ Seleccionada", fg_color="#4CAF50")
    
    def al_seleccionar_datos_fuente(self, ruta_archivo: str):
        """Callback cuando se selecciona datos_fuente.xlsx"""
        try:
            self.ruta_datos_fuente = ruta_archivo
            df = pd.read_excel(ruta_archivo, dtype=str)
            self.df_datos_fuente = self.procesador.sanitizar_dataframe(df)
            
            num_clientes = len(self.df_datos_fuente)
            self.clientes_label.configure(
                text=f"✅ {num_clientes} clientes encontrados | {num_clientes} carpetas a crear"
            )
            
            self.mensajear_log(f"✅ Archivo datos_fuente.xlsx cargado: {num_clientes} clientes")
            
            # Validar campos requeridos
            requeridos = ['cuenta', 'nombre', 'gestion_efectiva']
            valido, error = self.procesador.validar_campos_dataframe(
                self.df_datos_fuente, requeridos, "datos_fuente.xlsx"
            )
            if not valido:
                self.mensajear_log(f"⚠️ {error}")
                
        except Exception as e:
            self.mensajear_log(f"❌ Error cargando datos_fuente.xlsx: {str(e)}")
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{str(e)}")
    
    def al_seleccionar_nuevos_datos(self, ruta_archivo: str):
        """Callback cuando se selecciona nuevos_datos.xlsx"""
        try:
            self.ruta_nuevos_datos = ruta_archivo
            df = pd.read_excel(ruta_archivo, dtype=str)
            self.df_nuevos_datos = self.procesador.sanitizar_dataframe(df)
            
            self.mensajear_log(f"✅ Archivo nuevos_datos.xlsx cargado: {len(self.df_nuevos_datos)} registros")
            
            # Validar campos requeridos
            requeridos = ['cuenta', 'gestion_efectiva']
            valido, error = self.procesador.validar_campos_dataframe(
                self.df_nuevos_datos, requeridos, "nuevos_datos.xlsx"
            )
            if not valido:
                self.mensajear_log(f"⚠️ {error}")
                
        except Exception as e:
            self.mensajear_log(f"❌ Error cargando nuevos_datos.xlsx: {str(e)}")
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{str(e)}")
    
    def al_seleccionar_audio_ivr(self, ruta_archivo: str):
        """Callback cuando se selecciona audio IVR"""
        self.ruta_audio_ivr = ruta_archivo
        self.mensajear_log(f"✅ Audio IVR seleccionado: {os.path.basename(ruta_archivo)}")
    
    def al_seleccionar_sms(self, ruta_archivo: str):
        """Callback cuando se selecciona sms.xlsx"""
        try:
            self.ruta_sms = ruta_archivo
            df = pd.read_excel(ruta_archivo, dtype=str)
            self.df_sms = self.procesador.sanitizar_dataframe(df)
            
            self.mensajear_log(f"✅ Archivo sms.xlsx cargado: {len(self.df_sms)} registros")
            
            # Validar campo requerido
            requerido = ['numero_credito']
            valido, error = self.procesador.validar_campos_dataframe(
                self.df_sms, requerido, "sms.xlsx"
            )
            if not valido:
                self.mensajear_log(f"⚠️ {error}")
                
        except Exception as e:
            self.mensajear_log(f"❌ Error cargando sms.xlsx: {str(e)}")
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{str(e)}")
    
    def al_seleccionar_consolidados(self, ruta_archivo: str):
        """Callback cuando se selecciona consolidados.xlsx"""
        try:
            self.ruta_consolidados = ruta_archivo
            df = pd.read_excel(ruta_archivo, dtype=str)
            # No sanitizar consolidados, mantener nombres originales para la ruta
            self.df_consolidados = df
            
            # Solo quitar espacios en blanco
            for col in self.df_consolidados.columns:
                if self.df_consolidados[col].dtype == 'object':
                    self.df_consolidados[col] = self.df_consolidados[col].apply(
                        lambda x: x.strip() if isinstance(x, str) else x
                    )
            
            self.mensajear_log(f"✅ Archivo consolidados.xlsx cargado: {len(self.df_consolidados)} registros")
            
            # Validar campos requeridos (usando nombres originales)
            requeridos = ['dni', 'telefono', 'ruta', 'nombre_completo']
            faltantes = [f for f in requeridos if f not in self.df_consolidados.columns]
            if faltantes:
                self.mensajear_log(f"⚠️ consolidados.xlsx: Faltan campos {', '.join(faltantes)}")
                
        except Exception as e:
            self.mensajear_log(f"❌ Error cargando consolidados.xlsx: {str(e)}")
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{str(e)}")
    
    def mensajear_log(self, mensaje: str):
        """Agrega mensaje al log"""
        self.texto_log.insert("end", mensaje + "\n")
        self.texto_log.see("end")
        self.update_idletasks()
    
    def validar_entradas(self) -> bool:
        """Valida que todos los archivos necesarios estén seleccionados"""
        errores = []
        
        if not self.ruta_datos_fuente:
            errores.append("• datos_fuente.xlsx no seleccionado")
        
        if not self.ruta_nuevos_datos:
            errores.append("• nuevos_datos.xlsx no seleccionado")
        
        if not self.ruta_audio_ivr:
            errores.append("• Audio IVR no seleccionado")
        
        if not self.ruta_carpeta_salida:
            errores.append("• Carpeta de salida no seleccionada")
        
        if not self.entrada_nombre_carpeta.get().strip():
            errores.append("• Nombre de carpeta contenedora vacío")
        
        if errores:
            mensaje_error = "Por favor, complete los siguientes campos:\n\n" + "\n".join(errores)
            messagebox.showwarning("Campos incompletos", mensaje_error)
            self.mensajear_log("⚠️ Validación fallida: campos incompletos")
            return False
        
        return True
    
    def iniciar_procesamiento(self):
        """Inicia el procesamiento en un hilo separado"""
        if not self.validar_entradas():
            return
        
        # Deshabilitar botón de procesamiento
        self.boton_procesar.configure(state="disabled", text="⏳ Procesando...")
        
        # Limpiar log anterior
        self.texto_log.delete("1.0", "end")
        
        # Ejecutar en hilo separado para no bloquear la UI
        hilo = threading.Thread(target=self.procesar_evidencias)
        hilo.daemon = True
        hilo.start()
    
    def procesar_evidencias(self):
        """Procesa todas las evidencias"""
        try:
            self.mensajear_log("=" * 80)
            self.mensajear_log("🚀 INICIANDO PROCESAMIENTO DE EVIDENCIAS")
            self.mensajear_log("=" * 80)
            
            # Crear carpeta contenedora
            nombre_carpeta = self.entrada_nombre_carpeta.get().strip()
            salida_base = Path(self.ruta_carpeta_salida) / nombre_carpeta
            salida_base.mkdir(parents=True, exist_ok=True)
            os.utime(salida_base, None) # Asegurar fecha de hoy en la carpeta principal
            
            self.mensajear_log(f"\n📁 Carpeta de salida: {salida_base}")
            
            total_clientes = len(self.df_datos_fuente)
            self.mensajear_log(f"📊 Total de clientes a procesar: {total_clientes}\n")
            
            # Procesar cada cliente
            conteo_exito = 0
            for idx, (_, fila_cliente) in enumerate(self.df_datos_fuente.iterrows(), 1):
                self.mensajear_log(f"\n[{idx}/{total_clientes}] {'=' * 60}")
                
                exito = self.procesador.procesar_cliente(
                    fila_cliente,
                    self.df_nuevos_datos,
                    self.df_sms,
                    self.df_consolidados,
                    self.ruta_audio_ivr,
                    salida_base
                )
                
                if exito:
                    conteo_exito += 1
            
            # Resumen final
            self.mensajear_log("\n" + "=" * 80)
            self.mensajear_log("✅ PROCESAMIENTO COMPLETADO")
            self.mensajear_log("=" * 80)
            self.mensajear_log(f"📊 Clientes procesados exitosamente: {conteo_exito}/{total_clientes}")
            self.mensajear_log(f"📁 Carpetas creadas en: {salida_base}")
            self.mensajear_log("=" * 80)
            
            # Mostrar mensaje de éxito
            self.after(0, lambda: messagebox.showinfo(
                "Procesamiento completado",
                f"✅ Se procesaron {conteo_exito} de {total_clientes} clientes exitosamente.\n\n"
                f"Las evidencias se guardaron en:\n{salida_base}"
            ))
            
        except Exception as e:
            mensaje_error = f"❌ Error durante el procesamiento: {str(e)}"
            self.mensajear_log(f"\n{mensaje_error}")
            self.after(0, lambda: messagebox.showerror("Error", mensaje_error))
        
        finally:
            # Rehabilitar botón
            self.after(0, lambda: self.boton_procesar.configure(
                state="normal",
                text="🚀 PROCESAR EVIDENCIAS"
            ))


def principal():
    """Función principal"""
    app = AppEvidencias()
    app.mainloop()


if __name__ == "__main__":
    principal()
