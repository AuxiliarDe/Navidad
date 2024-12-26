import requests
import pandas as pd
import dropbox
import io
import time

# Credenciales de Dropbox
DROPBOX_REFRESH_TOKEN = "F-VLUwSNJu4AAAAAAAAAAfJ3RXw9B-nqwH3P-Wq0fZfhuzOW68uBBCVVe0CukrEH"
DROPBOX_APP_KEY = "trlclqnk0uot9ff"
DROPBOX_APP_SECRET = "6r434vqdhfee0m6"

# Función para obtener un nuevo access token usando el refresh token de Dropbox
def refresh_dropbox_access_token():
    url = "https://api.dropbox.com/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": DROPBOX_REFRESH_TOKEN,
        "client_id": DROPBOX_APP_KEY,
        "client_secret": DROPBOX_APP_SECRET
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

# Obtener un nuevo access token para Dropbox
dropbox_token = refresh_dropbox_access_token()
print(f"Nuevo access token de Dropbox: {dropbox_token}")

# Función para leer el access token desde una celda específica de un archivo Excel en Dropbox
def obtener_token_desde_excel(dropbox_token, file_path, celda, sheet_name='Llaves'):
    dbx = dropbox.Dropbox(dropbox_token)
    metadata, res = dbx.files_download(file_path)
    with io.BytesIO(res.content) as stream:
        df = pd.read_excel(stream, sheet_name=sheet_name, engine='openpyxl')
        row = int(celda[1:]) - 1  # Convertir la referencia de fila a índice (0-indexed)
        col = ord(celda[0].upper()) - ord('A')  # Convertir la referencia de columna a índice (0-indexed)
        token = df.iloc[row, col]  # Acceder a la celda usando iloc
    return token

# Especifica la celda desde donde quieres obtener el token
celda_token = "A7"  # Cambia esta celda según el token que necesites usar
file_path = "/Llaves.xlsx"  # Ruta al archivo en la raíz de Dropbox

# Obtener el token desde el archivo Excel
api_token = obtener_token_desde_excel(dropbox_token, file_path, celda_token)
print(f"Token obtenido desde Excel: {api_token}")
# Función para obtener los datos de la API de Dropi con paginación
def fetch_data(api_token, offset, limit):
    url = f"https://api.dropi.cl/api/orders/myorders?exportAs=orderByRow&orderBy=id&orderDirection=desc&result_number={limit}&start={offset}&textToSearch=&status=null&supplier_id=false&user_id=67185&from=2024-01-01&until=2024-12-31&filter_product=undefined&haveIncidenceProcesamiento=false&tag_id=&warranty=false&seller=null&filter_date_by=null&invoiced=null"
    headers = {
        "Authorization": "Bearer " + api_token,
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(url, headers=headers, timeout=300)
        response.raise_for_status()
        data = response.json()
        if "objects" in data:
            return data["objects"]
        else:
            print(f"No se encontraron objetos en el offset {offset}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud en el offset {offset}: {e}")
        return []

# Función para normalizar columnas anidadas
def explode_and_normalize(df, column, sub_columns):
    if (column in df) and (df[column].notna().any()):
        df = df.explode(column)
        normalized_df = pd.json_normalize(df[column])
        normalized_df.index = df.index  # Asegurar que los índices sean iguales
        normalized_df.columns = [f"{column}.{sub_col}" for sub_col in normalized_df.columns]
        df = df.drop([column], axis=1).join(normalized_df[sub_columns])
    return df

# Función para obtener los datos de la API de historywallet
def fetch_historywallet_data(api_token, offset, limit, from_date, until_date):
    url = f"https://api.dropi.cl/api/historywallet?orderBy=id&orderDirection=desc&result_number={limit}&start={offset}&textToSearch=&type=null&id=null&identification_code=null&user_id=67185&from={from_date}&until={until_date}&wallet_id=0"
    headers = {
        "Authorization": "Bearer " + api_token,
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(url, headers=headers, timeout=300)
        response.raise_for_status()
        data = response.json()
        if "objects" in data:
            return data["objects"], data
        else:
            print(f"No se encontraron objetos en el offset {offset}")
            return [], data
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud en el offset {offset}: {e}")
        return [], {}

# Parámetros de fecha (ajusta según sea necesario)
from_date = "2024-01-01"
until_date = "2024-12-31"

# Lista de offsets para la paginación
limit = 1000  # Número de registros por solicitud
offset = 0  # Offset inicial
max_records = 20000  # Número máximo de registros a obtener

# Obtener datos de la API de Dropi
all_data = []
while len(all_data) < max_records:
    data = fetch_data(api_token, offset, limit)
    if not data:
        print(f"Sin datos en el offset {offset}, terminando la búsqueda.")
        break
    all_data.extend(data)
    print(f"Datos obtenidos del offset {offset}: {len(data)} registros.")
    offset += limit
    time.sleep(1)  # Esperar 1 segundo antes de la siguiente solicitud
    if len(all_data) >= max_records:
        all_data = all_data[:max_records]
        break

# Verificar el contenido de all_data
if not all_data:
    print("No se obtuvieron datos de la API.")
else:
    # Crear DataFrame y seleccionar las columnas necesarias
    df = pd.DataFrame(all_data)
    
    # Asegurarse de que 'quantity' esté presente en el DataFrame
    if 'quantity' not in df.columns:
        df['quantity'] = None  # O agregar una columna vacía
    
    columns_to_select = ["id", "user_name", "supplier_name", "supplier", "status", "dir", "phone", "total_order", "name", 
                         "surname", "country", "state", "city", "zip_code", "created_at", "updated_at", 
                         "shipping_guide", "shipping_company", "shipping_amount", "supplier_amount", 
                         "amount_earned_dropshipper", "amount_earned_supplier", "dropshipper_amount_to_win", 
                         "quantity", "orderdetails", "tags"]
    df_filtered = df[columns_to_select]

    # Expandir y normalizar columnas anidadas
    df_flattened = explode_and_normalize(df_filtered, 'orderdetails', ['orderdetails.product.id', 'orderdetails.product.id_lista', 'orderdetails.product.name', 'orderdetails.product.name_in_order', 'orderdetails.quantity', 'orderdetails.price', 'orderdetails.supplier_price', 'orderdetails.shipping_amount' , 'orderdetails.amount_earned_dropshipper', 'orderdetails.order_id', 'orderdetails.variation.id'])

    # Convertir columnas específicas a números decimales
    decimal_columns = ["total_order", "shipping_amount", "amount_earned_dropshipper", "amount_earned_supplier", "dropshipper_amount_to_win", "quantity", "orderdetails.quantity",  'orderdetails.price', 'orderdetails.supplier_price', 'orderdetails.shipping_amount' , 'orderdetails.amount_earned_dropshipper']
    df_flattened[decimal_columns] = df_flattened[decimal_columns].apply(pd.to_numeric, errors='coerce')

    # Guardar en CSV en memoria
    csv_buffer = io.StringIO()
    df_flattened.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    print("Datos guardados en memoria")

    # Subir el archivo a Dropbox con reintentos
    dbx = dropbox.Dropbox(dropbox_token)
    max_retries = 5
    upload_successful = False
    for attempt in range(max_retries):
        try:
            dbx.files_upload(csv_buffer.getvalue().encode(), f"/A69.csv", mode=dropbox.files.WriteMode.overwrite)
            print(f"Archivo subido a Dropbox como /A69.csv")
            upload_successful = True
            break
        except dropbox.exceptions.InternalServerError as e:
            print(f"Error interno del servidor en Dropbox: {e}, reintentando ({attempt + 1}/{max_retries})")
            time.sleep(5)  # Esperar antes de reintentar
        except dropbox.exceptions.AuthError as e:
            print(f"Error de autenticación en Dropbox: {e}")
            break
        except Exception as e:
            print(f"Error al subir a Dropbox: {e}")
            break

if not upload_successful:
    print("Error al subir el archivo a Dropbox")

# Obtener datos de historywallet
all_historywallet_data = []
offset = 0  # Reiniciar el offset para la siguiente solicitud
while len(all_historywallet_data) < max_records:
    data, full_response = fetch_historywallet_data(api_token, offset, limit, from_date, until_date)
    if not data:
        print(f"Sin datos en el offset {offset}, terminando la búsqueda.")
        break
    all_historywallet_data.extend(data)
    print(f"Datos obtenidos del offset {offset}: {len(data)} registros.")
    offset += limit
    time.sleep(1)  # Esperar 1 segundo antes de la siguiente solicitud
    if len(all_historywallet_data) >= max_records:
        all_historywallet_data = all_historywallet_data[:max_records]
        break

# Verificar el contenido de los datos obtenidos
if not all_historywallet_data:
    print("No se obtuvieron datos de la API de historywallet.")
else:
    # Crear DataFrame y seleccionar las columnas necesarias
    df_historywallet = pd.DataFrame(all_historywallet_data)
    columns_to_select_historywallet = ["id", "amount", "type", "created_at", "previous_amount", "description", "order_id"]
    df_filtered_historywallet = df_historywallet[columns_to_select_historywallet]
    decimal_columns_historywallet = ["amount", "previous_amount"]
    for column in decimal_columns_historywallet:
        if column in df_filtered_historywallet.columns:
            df_filtered_historywallet.loc[:, column] = pd.to_numeric(df_filtered_historywallet[column], errors='coerce')

# Guardar en CSV en memoria
csv_buffer_historywallet = io.StringIO()
df_filtered_historywallet.to_csv(csv_buffer_historywallet, index=False)
csv_buffer_historywallet.seek(0)
print("Datos guardados en memoria")

# Subir el archivo a Dropbox con reintentos
upload_successful_historywallet = False
for attempt in range(max_retries):
    try:
        dbx.files_upload(csv_buffer_historywallet.getvalue().encode(), f"/A69H.csv", mode=dropbox.files.WriteMode.overwrite)
        print(f"Archivo subido a Dropbox como /A69H.csv")
        upload_successful_historywallet = True
        break
    except dropbox.exceptions.InternalServerError as e:
        print(f"Error interno del servidor en Dropbox: {e}, reintentando ({attempt + 1}/{max_retries})")
        time.sleep(5)  # Esperar antes de reintentar
        continue
    except dropbox.exceptions.AuthError as e:
        print(f"Error de autenticación en Dropbox: {e}")
        break
    except Exception as e:
        print(f"Error al subir a Dropbox: {e}, reintentando ({attempt + 1}/{max_retries})")
        time.sleep(5)  # Esperar antes de reintentar
        continue

if not upload_successful_historywallet:
    print("Error al subir el archivo a Dropbox")
