# Script para la consulta de datos en CASSANDRA y pivotarlos
# Los resultados se guardan en un fichero pickle

# Importación de modulos
from cassandra.cluster import Cluster
import datetime
import pandas as pd
import time
import os
from multiprocessing import Pool

# Configuración de parámetros
fecha_inicio = datetime.datetime(2017,1,6,0,0,0) # Fecha de inicio de la consulta
fecha_final = datetime.datetime(2018,12,31,0,0,0) # Fecha final de la consulta

# lista con los kks a consultar
ruta_preseleccion = r''
df_kks = pd.read_excel(ruta_preseleccion)
lista_kks = df_kks['KKS Cassandra'].to_list()

ruta_salida = r''

# Función para que las consultas devuelvan las filas como dataframes
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

# Configuración de la conexión a Cassandra
cluster = Cluster([''])
session = cluster.connect('tecnologia_energia')
session.default_fetch_size = None
session.default_timeout = 1000
session.row_factory = pandas_factory

# Preparación de la consulta
query = session.prepare("SELECT kks, time, value FROM spark_agregados_descarga WHERE KKS = ? and time >= ? and time < ?")

# Función para la consulta de datos, que se paralelizará
def consulta (kks, inicio, final):
    try:
        consulta = session.execute(query, [kks, inicio, final])
        df = consulta._current_rows
        df.drop('kks', axis=1, inplace=True)
        df.rename({'value': kks}, axis=1, inplace=True)
        df.set_index('time', inplace=True)
        return df

    except Exception as e:
        print('Error en la consulta del kks: ', kks, '\nPara los días: ', inicio, ' - ', final)
        print(e)
        return None

def main():
    # Inicio timer para ver la duración del script
    inicio_script = time.time()

    # Generación de la lista con los inputs. Cada consulta será sobre un kks. El número de días
    # está limitado para evitar timetout.
    inicio = fecha_inicio
    inputs = []
    while inicio <= fecha_final:
        final = inicio + datetime.timedelta(days=10)
        inputs_nuevos = [(kks, inicio, final ) for kks in lista_kks]
        inputs = inputs + inputs_nuevos
        inicio = final

    print('Consultas a realizar: ', len(inputs), ". Tiempo transcurrido: ", round((time.time() - inicio_script)/60,2), " minutos")

    # Ejecución de las consultas paralelizando
    with Pool() as p:
        resultados = p.starmap(consulta, inputs)
    
    print('Consultas ejecutadas: ', len(resultados), ". Tiempo transcurrido: ", round((time.time() - inicio_script)/60,2), " minutos")

    # Combinación de los resultados en un único dataframe
    datos = pd.DataFrame()
    for resultado in resultados:
        datos = datos.combine_first(resultado)

    print("Dimensiones de los datos: ", datos.shape, ". Tiempo transcurrido: ", round((time.time() - inicio_script)/60,2), " minutos")
    print(datos.head())
        
    # Guardo resultado en excel
    nombre_pickle = 'Datos_descarga_' + datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S') + '.pickle'
    datos.to_pickle(os.path.join(ruta_salida, nombre_pickle)) # Guardo el resultado en un fichero pickle

    # Cierre de la conexión
    cluster.shutdown()

    # Tiempo transcurrido
    print("Duración total: ", round((time.time() - inicio_script)/60,2), " minutos")

if __name__ =='__main__':
    main()