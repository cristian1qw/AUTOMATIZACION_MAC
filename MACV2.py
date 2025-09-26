import paramiko
from scp import SCPClient
import pandas as pd
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl import load_workbook
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

# Configuración de rutas
Aprobacion = "C:/Users/PC/INDEPENDENCE S.A/GerenciaTI - 0. Telco/1. CONECTIVIDAD/1. CONECTIVIDAD STARLINK/Automatizacion Macs/Aprobacion.xlsx"
ruta_clave = "C:/Users/PC/.ssh/id_rsa"
ruta_local = "C:/Users/PC/system.cfg"
ruta_salida = "C:/Users/PC/INDEPENDENCE S.A/GerenciaTI - 0. Telco/1. CONECTIVIDAD/1. CONECTIVIDAD STARLINK/Automatizacion Macs/Aprobados.xlsx"


# === Cargar archivos ===
df_macs = pd.read_excel(Aprobacion)
Tabla_aprobaciones = df_macs.dropna(subset=["MAC", "NOMBRE", "TORRE"]).tail(len(df_macs))
df_listado = pd.read_excel("C:/Users/PC/INDEPENDENCE S.A/GerenciaTI - 0. Telco/1. CONECTIVIDAD/1. CONECTIVIDAD STARLINK/Automatizacion Macs/Listado.xlsx")

def Tabla_Aprobado():
            
                        
            # === Obtener el último registro del libro de MACs ===
            for _, fila in Tabla_aprobaciones.iterrows():
                torre_objetivo = fila["TORRE"]
                nombre_objetivo = fila["NOMBRE"]
                mac_objetivo = fila["MAC"]
                Tipo_objetivo = fila["TIPO"]
                correo=fila["CORREO"]

                df_resultado = pd.DataFrame()
                df_filtrado = df_listado[df_listado["TORRE"].astype(str).str.strip() == str(torre_objetivo).strip()].copy()
                #crea un data frame con estas varibles
                df_filtrado["MAC"] = mac_objetivo
                df_filtrado["NOMBRE"] = nombre_objetivo
                df_filtrado["TORRE"] = torre_objetivo
                df_filtrado["ESTADO"] = "PENDIENTE"  # Estado 
                df_filtrado["TIPO"]=Tipo_objetivo
                df_filtrado["CORREO"]=correo
                
                df_resultado = pd.concat([df_resultado, df_filtrado[["IP","TIPO","MAC", "NOMBRE", "TORRE", "ESTADO","CORREO"]]], ignore_index=True)


                # Seleccionar columnas finales
                
               
                insertar_tabla("Sheet1",df_resultado)


            Hoja1 = pd.read_excel(ruta_salida,sheet_name="Sheet1")
            Hoja2 = pd.read_excel(ruta_salida,sheet_name="Sheet2")
            Hoja3=pd.DataFrame(Hoja1)
            Hoja4=pd.DataFrame(Hoja2)   

            for index, fila in Hoja3.iterrows():
                    ip = fila["IP"]
                    mac = fila["MAC"]
                    existe = not Hoja4[(Hoja4["IP"] == ip) & (Hoja4["MAC"] == mac)].empty
                    
                    if existe:
                            actualizar_estado_en_tabla(ip, mac, "ya existe", "Sheet1")
                    else: 
                         print("no existe")
                                   
def recorrer():
                df = pd.read_excel(ruta_salida,sheet_name="Sheet1")
                df = df.dropna(subset=["MAC"])
                print("recorriendoi")
                if 'IP' not in df.columns:
                        print("El archivo Excel debe contener una columna 'IP'")
                else:                    
                    print("tiene columna ip")
                    aprobados=df[df["ESTADO"].isin(["PENDIENTE"])]
                    dispositivos = aprobados.groupby('IP')
                    for ip, grupo in dispositivos:
                            print(f"\n🔧 Procesando IP: {ip}")
                            procesar_dispositivo(ip,grupo)     
        
def procesar_dispositivo(ip, grupo):
    print(f"\n🔧 Procesando dispositivo {ip}")
    usuario = "ubnt"
    ruta_remota = "/tmp/system.cfg"
    
    carpeta_temporal = f"C:/Users/PC/temp_cfgs/"
    os.makedirs(carpeta_temporal, exist_ok=True)
    ruta_local = os.path.join(carpeta_temporal, f"system_{ip.replace('.', '_')}.cfg")

    try:
        clave = paramiko.RSAKey.from_private_key_file(ruta_clave)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            ip,
            username=usuario,
            pkey=clave,
            disabled_algorithms=dict(pubkeys=["rsa-sha2-256", "rsa-sha2-512"]),
            timeout=10
        )
        print(f"✅ Conexión establecida con {ip}")

        with SCPClient(ssh.get_transport()) as scp:
            scp.get(ruta_remota, ruta_local)
        print(f"📥 Archivo descargado: {ruta_local}")

        with open(ruta_local, "r", encoding="utf-8") as f:
            contenido = f.read()

        mac_existentes = set(m.group(1).lower() for m in re.finditer(r"mac_acl\.\d+\.mac=(.*?)\n", contenido))
        indices = [int(m.group(1)) for m in re.finditer(r"wireless\.1\.mac_acl\.(\d+)\.mac=", contenido)]
        siguiente = max(indices, default=-1) + 1

        nuevas_macs = 0
        for idx, fila in grupo.iterrows():
            mac = fila["MAC"].strip().lower()
            if mac in mac_existentes:
                print(f"  [{ip}] MAC ya existe, se omite: {mac}")
                actualizar_estado_en_tabla(ip,mac,"REPLICADO","Sheet1")
                
                
                continue

            nombre = str(fila.get("NOMBRE", "")).strip()
            bloque = f"""
wireless.1.mac_acl.{siguiente}.mac={mac}
wireless.1.mac_acl.{siguiente}.status=enabled
wireless.1.mac_acl.{siguiente}.comment={nombre}

"""
            contenido += bloque
            siguiente += 1
            nuevas_macs += 1
            print(f"  [{ip}] MAC agregada: {mac} - {nombre}")

            actualizar_estado_en_tabla(ip,mac,"APROBADO","Sheet1")
            
           

        with open(ruta_local, "w", encoding="utf-8") as f:
            f.write(contenido)

        if nuevas_macs > 0:
            with SCPClient(ssh.get_transport()) as scp:
                scp.put(ruta_local, ruta_remota)

            comandos = [
                "lock /var/lock/.system.cfg.lock",
                "cfgmtd -f /etc/system.cfg -w",
                "ebtables -F",
                "wlanconfig ath0 listmac",
                "iwpriv ath0 maccmd 1",
                "iwpriv ath0 maccmd 4",
                "lock -u /var/lock/.system.cfg.lock"
            ]
                                      
            for cmd in comandos:
                stdin, stdout, stderr = ssh.exec_command(cmd)
                print(stdout.read().decode())
                print(stderr.read().decode())
            
            print(f"✅ [{ip}] {nuevas_macs} nuevas MACs configuradas")
        else:
            print(f"ℹ️ [{ip}] No se agregaron nuevas MACs")

        return ip, True, f"{nuevas_macs} MACs agregadas"

    except Exception as e:
        print(f"❌ [{ip}] Error: {str(e)}")
        for idx, fila in grupo.iterrows():
            mac = fila["MAC"].strip().lower()
            actualizar_estado_en_tabla(ip,mac,"ERROR","Sheet1")
           
            
        return ip, False, str(e)

    finally:
        if 'ssh' in locals():
            ssh.close()
        if os.path.exists(ruta_local):
            os.remove(ruta_local)

def insertar_tabla(N_hoja,df_resultado):
                
                wb = load_workbook(ruta_salida)
                ws = wb[N_hoja] 
               
                tabla = list(ws.tables.values())[0]
               

                # Determinar la siguiente fila disponible
                fila_siguiente = ws.max_row + 1

                # Agregar los datos nuevos debajo
                for fila in df_resultado.values.tolist():
                    for col_idx, valor in enumerate(fila, start=1):
                        ws.cell(row=fila_siguiente, column=col_idx, value=valor)
                    fila_siguiente += 1

                # Actualizar el rango de la tabla
                inicio, fin = tabla.ref.split(":")
                col_fin = ''.join([c for c in fin if not c.isdigit()])  # Solo letras
                nueva_ref = f"{inicio}:{col_fin}{fila_siguiente-1}"
                tabla.ref = nueva_ref

                print(f"📊 Tabla '{tabla.name}' actualizada: {tabla.ref}")

                # Guardar
                wb.save(ruta_salida)
                print("✅ Datos agregados y tabla actualizada en Excel")

def actualizar_estado_en_tabla(ip_buscar, mac_buscar, estado,N_hoja):
        wb = load_workbook(ruta_salida)
        ws = wb[N_hoja]

        # Mapear encabezados a posiciones
        encabezados = {cell.value: idx for idx, cell in enumerate(ws[1], start=1)}
        col_ip = encabezados.get("IP")
        col_mac = encabezados.get("MAC")
        col_estado = encabezados.get("ESTADO")

        if not all([col_ip, col_mac, col_estado]):
            print("No se encontraron columnas IP, MAC o ESTADO en la tabla.")
            return

        # Recorrer filas desde la segunda (sin encabezados)
        for fila in ws.iter_rows(min_row=2):
            valor_ip = str(fila[col_ip - 1].value).strip().lower()
            valor_mac = str(fila[col_mac - 1].value).strip().lower()

            if valor_ip == ip_buscar.strip().lower() and valor_mac == mac_buscar.strip().lower():
                fila[col_estado - 1].value = estado
                break
                

        wb.save(ruta_salida)
        return   

def borrado_aprobaciones(tabla,N_hoja):

    #Borrado    
    wb = load_workbook(tabla)
    ws = wb[N_hoja]  # Si tienes varias hojas puedes usar wb["NombreHoja"]
    if ws.max_row > 1:
        ws.delete_rows(2, ws.max_row - 1)  # Desde fila 2 hasta la última

    # Guardar cambios
        wb.save(tabla)
def correo(MAC,torre,nombre,dominio,tipo,contraseña):
    
        

        msg = MIMEMultipart("related")
        msg["Subject"] = f"INCLUSION DE MAC - {torre}"
        msg["From"] = ("soporteticampo@independence.com.co")
        msg["To"] = dominio
        msg["Cc"] = "mesadeayuda@independence.com.co, cfgarzon@independence.com.co"
        
        
        

        html = f"""\
    <html>
      <body>
        <p><b>Buen día,</b><br><br>
           Se confirma inclusión de la MAC solicitada:<br><br><br>
    
           <b>Nombre:</b> {nombre} <br>
           <b>MAC:</b> {MAC} <br>
           <b>Dispositivo:</b> {tipo} <br>
           <b>Torre:</b> {torre} <br><br>
            
           a continuación, relacionamos la información para establecer conexión: <br><br><br>

           <b>Nombre de la red :</b> IND{torre} <br>
           <b>Contraseña:</b> IND{contraseña} <br><br><br>
           

           <u><i>No olvide desactivar la opción de <b>MAC aleatoria</b> en el dispositivo para poder establecer conexión.</i></u><br><br>
           En caso de no lograr conexion siga lo siguientes pasos:</b> 
           <br><br>
           <div>
      <img src="cid:WINDOWS" style="display:inline; width:210px; height:auto; margin-right:10px;">
      <img src="cid:ANDROID" style="display:inline; width:200px; height:auto; margin-right:10px;">
      <img src="cid:IOS" style="display:inline; width:200px; height:auto;margin-right:10px;">
    </div>
        </p>
      </body>
    </html>
    """
        parte_html = MIMEText(html, "html")
        msg.attach(parte_html)
        ruta_imagen={"WINDOWS":"C:/Users/PC/INDEPENDENCE S.A/GerenciaTI - 0. Telco/1. CONECTIVIDAD/1. CONECTIVIDAD STARLINK/Automatizacion Macs/MAC Windows.JPG",
                     "ANDROID":"C:/Users/PC/INDEPENDENCE S.A/GerenciaTI - 0. Telco/1. CONECTIVIDAD/1. CONECTIVIDAD STARLINK/Automatizacion Macs/MAC ANDROID.JPG",
                     "IOS":"C:/Users/PC/INDEPENDENCE S.A/GerenciaTI - 0. Telco/1. CONECTIVIDAD/1. CONECTIVIDAD STARLINK/Automatizacion Macs/MAC iOS.JPG"}
        for cid, ruta in ruta_imagen.items():
            with open(ruta, "rb") as f:
                img = MIMEImage(f.read(), _subtype="jpeg")

                img.add_header("Content-ID",  f"<{cid}>")
                img.add_header("Content-Disposition", "inline", filename=ruta.split("\\")[-1])

                msg.attach(img)


        with smtplib.SMTP("smtp.office365.com", 587) as server:
            server.starttls()
            server.login("soporteticampo@independence.com.co", "1n7R4&*t1&")
            server.send_message(msg)
            
            print("✅ Correo enviado para activar el flujo")

            
if __name__ == "__main__":
    
    borrado_aprobaciones(ruta_salida,"Sheet1") 
    Tabla_Aprobado()
    recorrer()
    fallos=[]
    historico=[]
    df2 = pd.read_excel(ruta_salida,"Sheet1")
    
    
    agrupados=df2.groupby(["IP","TIPO","MAC","TORRE","NOMBRE","ESTADO","CORREO"]) 
              
    for (ip,tipo,mac,torre,nombre,estado,dominio), grupo in agrupados:
         if grupo["ESTADO"].isin(["APROBADO", "REPLICADO"]).any():

               
                historico.append({"IP":ip,
                                  "TIPO":tipo,
                                  "MAC":mac,
                               "NOMBRE":nombre,
                               "TORRE":torre,
                               "ESTADO":estado,
                               "CORREO":dominio})
                print("aprobado correo enviado",mac, torre)
                
         else:
                
                
                fallos.append({"MAC":mac,
                               "TIPO":tipo,
                               "NOMBRE":nombre,
                               "TORRE":torre,
                               "CORREO":dominio})
    filtro = df2[df2["ESTADO"].isin(["APROBADO", "REPLICADO"])]
    grupos = filtro.groupby(["TORRE","TIPO","NOMBRE", "MAC", "CORREO"]).size().reset_index()
     
    for _, fila in grupos.iterrows():
        torre = fila["TORRE"]
        torre2=torre.replace(" ", "-")
        contraseña=torre2.replace("-", "")
        nombre = fila["NOMBRE"]
        mac = fila["MAC"]
        correo2 = fila["CORREO"]
        tipo=fila["TIPO"]
        correo(mac,torre2,nombre,correo2,tipo,contraseña)
    
    df_fallos=pd.DataFrame(fallos)
    df_historico=pd.DataFrame(historico) 
      
    insertar_tabla("fallos",df_fallos)
    insertar_tabla("Sheet2",df_historico)
    borrado_aprobaciones(Aprobacion,"Sheet1")
    print("\n📊 Proceso completado. Resultados guardados en:", ruta_salida)
    
    
