import csv
from cassandra.cluster import Cluster
import os

# Ubicacion del archivo CSV con el contenido provisto por la catedra
directorio_actual = os.path.dirname(os.path.abspath(__file__))
archivo_entrada =  os.path.join(directorio_actual, '..', 'datos', 'full_export.csv')
nombre_archivo_resultado_ejercicio = os.path.join(directorio_actual, 'tp3_ej01.txt')

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = {
    'cassandraurl': 'localhost',
    'cassandrapuerto': 9042
}


# Funcion que dada la configuracion y ubicacion del archivo, carga la base de datos, genera el reporte, y borra la
# base de datos
def ejecutar(file, conn):
    import time

    start = time.time()
    db = inicializar(conn)
    df_filas = csv.DictReader(open(file, "r", encoding="utf-8"))
    count = 0
    startbloque = time.time()
    for fila in df_filas:
        procesar_fila(db, fila)
        count += 1
        if 0 == count%100:
            endbloque = time.time()
            tiempo = endbloque-startbloque
            print(str(count) + " en " + str(tiempo) + " segundos")
            startbloque = time.time()
    generar_reporte(db)
    finalizar(db)
    end = time.time()
    print("tiempo total en segundos")
    print(end - start)


# Funcion que dado un archivo abierto y una linea, imprime por consola y guarda al final de archivo esa linea
def grabar_linea(archivo, linea):
    print(linea)
    archivo.write(str(linea) + '\n')


def inicializar(conn):
    cassandra_session = Cluster(contact_points=[conn["cassandraurl"]], port=conn["cassandrapuerto"]).connect()
    # crear db
    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS tp3_ej01 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    cassandra_session.set_keyspace('tp3_ej01')
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS deportistas (
            id INT PRIMARY KEY,
            nombre TEXT
        );                   
    """)
    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    # insertar elemento en entidad para el ejercicio actual
    id = int(fila['id_deportista'])
    nombre = fila['nombre_deportista']
    query = """
        INSERT INTO tp3_ej01.deportistas (id, nombre)
        VALUES (%s, %s)
    """
    db.execute(query, (id, nombre))

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    # archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # luego para cada linea generada como reporte:
    # grabar_linea(archivo, linea)
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")
    grabar_linea(archivo, "id - nombre")
    query = """
        SELECT id, nombre FROM deportistas WHERE id IN (10, 20, 30)
    """
    filas = db.execute(query)
    for fila in filas:
        grabar_linea(archivo, f"{fila.id} - {fila.nombre}")


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    # Borrar la estructura de la base de datos
    db.execute("DROP KEYSPACE IF EXISTS tp3_ej01;")


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
