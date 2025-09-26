import paramiko
from scp import SCPClient
import pandas as pd
import traceback
import re
import os
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
# Cargar el archivo .env

smtp_user = os.getenv("SMTP_USER")
smtp_password = os.getenv("SMTP_PASS")
ssh_key = os.getenv("SSH_KEY")


def Tabla_Aprobado(df_resultado):
            
                        
            # === Obtener el último registro del libro de MACs ===
            for _, fila in Tabla_aprobaciones.iterrows():
                torre_objetivo = fila["TORRE"]
                nombre_objetivo = fila["NOMBRE"]
                mac_objetivo = fila["MAC"]
                Tipo_objetivo = fila["TIPO"]
                correo=fila["CORREO"]
                
                df_filtrado = df_listado[df_listado["TORRE"].astype(str).str.strip() == str(torre_objetivo).strip()].copy()
                #crea un data frame con estas varibles
                df_filtrado["MAC"] = mac_objetivo
                df_filtrado["NOMBRE"] = nombre_objetivo
                df_filtrado["TORRE"] = torre_objetivo
                df_filtrado["ESTADO"] = "PENDIENTE"  # Estado 
                df_filtrado["TIPO"]=Tipo_objetivo
                df_filtrado["CORREO"]=correo
            
                df_resultado = pd.concat([df_resultado, df_filtrado[["IP","TIPO","MAC", "NOMBRE", "TORRE", "ESTADO","CORREO"]]], ignore_index=True)
                
                
            df_resultado = df_resultado.drop_duplicates()
            print(df_resultado.head(5))    # Seleccionar columnas finales              
            
            Hoja2 = pd.read_excel(ruta_salida,sheet_name="Sheet2")
            Hoja4=pd.DataFrame(Hoja2)

            #recorre el df por cada fila
            for index, fila in df_resultado.iterrows():
                    ip = fila["IP"]
                    mac = fila["MAC"]
                    #compara el df con la hoja2 de aprobados
                    existe = not Hoja4[(Hoja4["IP"] == ip) & (Hoja4["MAC"] == mac)].empty
                    #agrega existe si se cumple la condicion
                    if existe:
                            
                            #ingresa a la columna "ESTADO"
                            df_resultado.loc[index, "ESTADO"] = "Ya existe"
                            df_resultado = df_resultado.drop(index)
                            print("ya existe")
                            
                    else: 
                            print("no existe")
                        
            if df_resultado.empty:
                 print("No hay mac por incluir")
            else:     
                recorrer(df_resultado)  
def recorrer(df_resultado):
            
                print("Recorriendo")

                if 'IP' not in df_resultado.columns:
                        print("El archivo Excel debe contener una columna 'IP'")
                else:                    
                    print("tiene columna ip")

                       #Limpia valores vacios y agrupa las mac que esten en pendiente
                            
                    df_resultado = df_resultado.dropna(subset=["MAC"])
                    aprobados=df_resultado[df_resultado["ESTADO"].isin(["PENDIENTE","ERROR"])]
                    dispositivos = aprobados.groupby('IP')
                   
                   
                    for ip, grupo in dispositivos:
                            #inicia el proceso de inclusion de mac
                            print(f"\n🔧 Procesando IP: {ip}")
                            procesar_dispositivo(ip,grupo,df_resultado)
                    
                    #Agrupa aprobados-replicados, y errores-pendientes
                    filtro = df_resultado[df_resultado["ESTADO"].isin(["APROBADO", "REPLICADO"])]
                    filtro2= df_resultado[df_resultado["ESTADO"].isin(["ERROR", "PENDENTE"])]
                    grupos = filtro.groupby(["TORRE","TIPO","NOMBRE", "MAC", "CORREO"]).size().reset_index()
                    error = filtro2.groupby(["IP","TORRE","TIPO","NOMBRE", "MAC", "CORREO"]).size().reset_index()


                        # Se envia los correos para los aprobados
                    for _, fila in grupos.iterrows():
                            torre = fila["TORRE"]
                            torre2=torre.replace(" ", "-")
                            contraseña=torre2.replace("-", "")
                            nombre = fila["NOMBRE"]
                            mac = fila["MAC"]
                            correo2 = fila["CORREO"]
                            tipo=fila["TIPO"]
                            correo(mac,torre2,nombre,correo2,tipo,contraseña)
                    for _, fila in error.iterrows():
                            torre = fila["TORRE"]
                            torre2=torre.replace(" ", "-")
                            contraseña= fila["IP"]
                            nombre = fila["NOMBRE"]
                            mac = fila["MAC"]
                            correo2 = "cfgarzon@independence.com.co"
                            tipo=fila["TIPO"]
                            correoerrores(mac,torre2,nombre,correo2,tipo,contraseña)

                # se insertan los resultados a la tabla aprobados 
                
                insertar_tabla("Sheet2",filtro)
                insertar_tabla("fallos",filtro2)
def procesar_dispositivo(ip, grupo,df_resultado):
    print(f"\n🔧 Procesando dispositivo {ip}")
    usuario = "ubnt"
    ruta_remota = "/tmp/system.cfg"
    
    carpeta_temporal = f"C:/Users/PC/temp_cfgs/"
    os.makedirs(carpeta_temporal, exist_ok=True)
    ruta_local = os.path.join(carpeta_temporal, f"system_{ip.replace('.', '_')}.cfg")

    try:
        clave = paramiko.RSAKey.from_private_key_file(ssh_key)
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
                df_resultado.loc[idx, "ESTADO"] = "REPLICADO"
                
                
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

            
            df_resultado.loc[idx, "ESTADO"] = "APROBADO"
           

        with open(ruta_local, "w", encoding="utf-8") as f:
            f.write(contenido)

        if nuevas_macs > 0:
            with SCPClient(ssh.get_transport()) as scp:
                scp.put(ruta_local, ruta_remota)

            comandos = [
                
    "lock /var/lock/.system.cfg.lock",
    "cp /tmp/system.cfg /etc/system.cfg",
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
            
            df_resultado.loc[idx, "ESTADO"] = "ERROR"
            
        return ip, False, str(e)

    finally:
        if 'ssh' in locals():
            ssh.close()
        if os.path.exists(ruta_local):
            os.remove(ruta_local)
def insertar_tabla(N_hoja,df_resultado):
                # Cargar libro y buscar última fila usada
            wb = load_workbook(ruta_salida)
            hoja = wb[N_hoja]
            fila_inicio = hoja.max_row  # última fila con datos
            with pd.ExcelWriter(ruta_salida, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
                df_resultado.to_excel(writer, sheet_name=N_hoja, index=False, header=False, startrow=fila_inicio)
           
            print("✅ Filas incluidas") 
def borrado_aprobaciones(tabla,N_hoja):

    wb = load_workbook(tabla)
    ws = wb[N_hoja]
    max_row = ws.max_row
    for row in range(ws.max_row, 1, -1):
        valores = [cell.value for cell in ws[row]]
    # Si TODA la fila está vacía
        if all(v is None or str(v).strip() == "" for v in valores):
            ws.delete_rows(row, 1)
# Borrar todas las filas excepto la primera (encabezado)
    if max_row > 1:
        ws.delete_rows(2, max_row - 1)
    wb.save(tabla)
    print("✅ Filas eliminadas, se conserva la primera fila como encabezado")
def borrado_duplicados(tabla,N_hoja):
    wb = load_workbook(tabla)
    ws = wb[N_hoja]    
    vistos = set()
    filas_a_borrar = []

    for idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
                # Convertimos cada valor de la fila a string para comparación
                fila_como_texto = tuple(str(x) if x is not None else "" for x in row)

                if fila_como_texto in vistos:
                    filas_a_borrar.append(idx)
                else:
                    vistos.add(fila_como_texto)

            # Borrar de abajo hacia arriba
    for idx in reversed(filas_a_borrar):
                ws.delete_rows(idx)
    print("✅ Duplicados eliminados")
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
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
def correoerrores(MAC,torre,nombre,dominio,tipo,contraseña):            
        msg = MIMEMultipart("related")
        msg["Subject"] = f"FALLA INCLUSION DE MAC - {torre} - IP {contraseña}"
        msg["From"] = ("soporteticampo@independence.com.co")
        msg["To"] = "cfgarzon@independence.com.co"
     
    
        html = f"""\
    <html>
      <body>
        <p><b>Buen día,</b><br><br>
           Se confirma FALLA inclusión de la MAC solicitada:<br><br><br>
    
           <b>Nombre:</b> {nombre} <br>
           <b>MAC:</b> {MAC} <br>
           <b>Dispositivo:</b> {tipo} <br>
           <b>Torre:</b> {torre} <br><br>
           <b>IP:</b> {contraseña} <br><br>
      </body>
    </html>
    """
        parte_html = MIMEText(html, "html")
        msg.attach(parte_html)

        with smtplib.SMTP("smtp.office365.com", 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            
            print("✅ Correo enviado para activar el flujo")
     
if __name__ == "__main__":
    try:
       
        df_macs = pd.read_excel(Aprobacion)
        Tabla_aprobaciones = df_macs.dropna(subset=["MAC", "NOMBRE", "TORRE"]).tail(len(df_macs))
        df_listado = pd.read_excel("C:/Users/PC/INDEPENDENCE S.A/GerenciaTI - 0. Telco/1. CONECTIVIDAD/1. CONECTIVIDAD STARLINK/Automatizacion Macs/Listado.xlsx")
        df_resultado = pd.DataFrame()


        Tabla_Aprobado(df_resultado)
        
        print("Recorriendo Fallos")
        df=pd.read_excel(ruta_salida,sheet_name="fallos")
        borrado_aprobaciones(ruta_salida,"fallos")
        recorrer(df) 
        borrado_aprobaciones(Aprobacion,"Sheet1")
        borrado_duplicados(ruta_salida,"Sheet2")
        print("\n📊 Proceso completado. Resultados guardados en:", ruta_salida)
        print("contrase",smtp_password)
            
    except Exception as e:
    # Capturar stacktrace completo
        error_detalle = traceback.format_exc()

        # Construir correo
        msg = MIMEMultipart("related")
        msg["Subject"] = "⚠️ FALLA EN FLUJO"
        msg["From"] = "soporteticampo@independence.com.co"
        msg["To"] = "cfgarzon@independence.com.co"

        html = f"""
        <html>
        <body>
            <p><b>Buen día,</b><br><br>
            Se confirma <b>FALLA EN FLUJO</b>.<br><br>
            <b>Error:</b> {e}<br><br>
            <pre>{error_detalle}</pre>
        </body>
        </html>
        """
        msg.attach(MIMEText(html, "html"))

        # Enviar correo
    
        with smtplib.SMTP("smtp.office365.com", 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)

            print("✅ Correo enviado para activar el flujo")
        
