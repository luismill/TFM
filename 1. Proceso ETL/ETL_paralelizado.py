## Script para cargar archivos csv a Cassandra de forma paralelizada

# Importación de modulos
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import os
import datetime
import time
import pandas as pd
from multiprocessing import Pool
import dask.dataframe as dd
    
# Creación de todas las rutas de csv y los inputs
def list_inputs (rootpath, file_csv = None, list_csv = None):
    ### Devuelve una lista con todos los inputs de la función de carga paralelizada, el path completo de cada csv
    ### Inputs:
    ###     rootpath: ruta de la carpeta donde están los csv a cargar. Si no se pasa ningún argumento más, se leen todos los archivos de esta carpeta
    ###     file_csv: ruta relativa de un csv con la lista de todos los archivos csv a cargar
    ###     list_csv: lista con los nombres de todos los archivos csv a cargar

    if file_csv:
        df_csv = pd.read_csv(file_csv, header=None)
        list_csv = df_csv[0].to_list()
    elif list_csv:
        list_csv = list_csv
    else:
        list_csv = os.listdir(rootpath)
    inputs = []
    for csv in list_csv:
        path = os.path.join(rootpath,csv)
        inputs.append(path)
    return inputs
    
def load_file(path, kks = None):
    ### Función que lee el archivo situado en 'path' de un kks y lo carga a Cassandra
    ### inputs:
    ###        'path': ruta del fichero csv que contiene los datos para cargar a Cassandra
    ###        'kks': nombre del kks al que corresponden los datos. Si no se pasa el kks, se obtiene del nombre del archivo suponiendo este formato 'kks_xx-xx-xx.csv'
    ### 
    ### Se devuelve la ruta del fichero csv solo si la carga es exitosa. Si no es exitosa, se recoge la excepción y se devuelve 'None'
    
    # Comprobación que el fichero no está vacío
    if os.stat(path).st_size < 100:
        return None
    if not kks:
        file = path.split('\\')[-1]
        kks = file[:-4]

    # Se intenta la carga de datos y se devuelve la ruta si es exitosa
    try:
        csv = dd.read_csv(path, sep=';', header=None, names=['TimeStamp', 'Value'], dtype={'Value': 'float64'}, parse_dates=[0], dayfirst = True, decimal=',', engine='python')
        sin_duplicados = csv.groupby('TimeStamp', sort=False).first().reset_index()
        sin_duplicados['Date'] = sin_duplicados['TimeStamp'].dt.date
            
        # Se separan los datos por fechas para subir a cassandra
        fechas = list(sin_duplicados.Date.unique())
        for fecha in fechas:
            dia = sin_duplicados[sin_duplicados.Date == fecha]

            # Inicialización del Batch que agrupará las inserciones.
            # Para que sea eficiente, los inserts de cada batch tienen que tener la misma partition key (KKS y date)
            batch = BatchStatement()
            for row in dia.itertuples():
                batch.add(insert, (kks, row.Date, row.TimeStamp.to_pydatetime(), row.Value))

            # Ejecución del batch
            session.execute(batch)
        
        return path

    # Si se produce un fallo en la carga, se imprime por pantalla la ruta del fichero que fallo
    except Exception as e:
        print('Error cargando el archivo ', path)
        print(e)
        return None

def select_files_to_load(total_files, already_loaded_files):
    ### Función que selecciona los archivos csv en 'root' que todavía no han sido cargados a Cassandra.
    ### inputs:
    ###        'total_files': ruta del fichero csv que contiene una lista con todos los ficheros csv a cargar a Cassandra
    ###        'already_loaded_files': lista con las rutas de ficheros csv que contienen las listas con todos los ficheros csv ya cargados anteriormente a Cassandra
    ###
    ### Se devuelve una lista con los nombres de los ficheros a cargar a Cassandra

    df_total_files = pd.read_csv(total_files, names=['file'])
    list_csv_total = df_total_files.file.to_list()
    csv_loaded = []
    for file in already_loaded_files:
        df_csv_loaded = pd.read_csv(file, skiprows=1, names=['path'], usecols=[1])
        file_loaded = [path.split("\\")[-1] for path in df_csv_loaded.path]
        csv_loaded += file_loaded
    csv_to_load = list(set(list_csv_total) - set(csv_loaded))
    return csv_to_load

def main():
    # Inicio timer para ver la duración del script
    inicio = time.time()

    print('Comienza el script')
 
    # Selección de ficheros a cargar
    # csv_files_to_load = ...
    # already_loaded_files = [...]
    # files_to_load = select_files_to_load(csv_files_to_load, already_loaded_files) para que devuelva una lista con los ficheros que faltan por cargar.

    # Creación de las rutas y kks para los insert
    root = ''
    inputs = list_inputs(rootpath=root)
    archivos = len(inputs)
    cargados = []

    print('Archivos a cargar: ', archivos)
    print('Comienza la carga')

    # Ejecución de los inserts paralelizando
    with Pool() as p:
        for result in p.map(load_file, inputs):
            cargados.append(result)

    # Cierre de la conexión
    cluster.shutdown()

    # Guardo los archivos cargados en CSV
    nombre_csv = 'archivos_cargados_' + str(datetime.date.today()) + '.csv'
    df_cargados = pd.DataFrame(cargados)
    df_cargados.dropna(inplace=True)
    print('Archivos cargados: ', len(df_cargados.index))
    root_archivos_cargados = ""
    path_archivos_cargados = os.path.join(root_archivos_cargados, nombre_csv)
    df_cargados.to_csv(path_archivos_cargados) # Guardo el resultado en un CSV

    # Tiempo transcurrido
    final = time.time()
    duracion = (final - inicio)/60
    print("Duración total: ", duracion, " minutos")

# Configuración de la conexión a Cassandra
cluster = Cluster([''])
session = cluster.connect('tecnologia_energia')

# Preparación de la instrucción para insertar datos
insert = session.prepare("INSERT INTO medidas (KKS, date, time, value) VALUES (?, ?, ?, ?)")

if __name__ == '__main__':

    main()
  