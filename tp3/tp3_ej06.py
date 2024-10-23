import csv
from cassandra.cluster import Cluster
import os

# Ubicacion del archivo CSV con el contenido provisto por la catedra
directorio_actual = os.path.dirname(os.path.abspath(__file__))
archivo_entrada =  os.path.join(directorio_actual, '..', 'datos', 'full_export.csv')
nombre_archivo_resultado_ejercicio = os.path.join(directorio_actual, 'tp3_ej06.txt')

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
        CREATE KEYSPACE IF NOT EXISTS tp3_ej06 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    cassandra_session.set_keyspace('tp3_ej06')
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS especialidad_torneos (
            id_especialidad INT,
            id_torneo INT,
            nombre_especialidad TEXT,
            nombre_torneo TEXT,
            tipo TEXT,
            PRIMARY KEY (id_especialidad, id_torneo)
        );                  
    """)
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS marcas (
            id_deportista INT,
            id_especialidad INT,
            id_torneo INT,
            nombre_deportista TEXT,
            marca INT,
            intento INT,
            PRIMARY KEY ((id_especialidad, id_torneo), marca, id_deportista)
        );                  
    """)
    
    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    # insertar elemento en entidad para el ejercicio actual
    id_deportista = int(fila['id_deportista'])
    id_especialidad = int(fila['id_especialidad'])
    id_torneo = int(fila['id_torneo'])
    nombre_deportista = fila['nombre_deportista']
    nombre_especialidad = fila['nombre_especialidad']
    nombre_torneo = fila['nombre_torneo']
    nombre_tipo_especialidad = fila['nombre_tipo_especialidad']
    marca = int(fila['marca'])
    intento = int(fila['intento'])
    
    query_especialidad_torneos = """
        INSERT INTO especialidad_torneos (id_especialidad, id_torneo, nombre_especialidad, nombre_torneo, tipo)
        VALUES (%s, %s, %s, %s, %s)
    """
    db.execute(query_especialidad_torneos, (id_especialidad, id_torneo, nombre_especialidad, nombre_torneo, nombre_tipo_especialidad))
    
    query_marca = """
        INSERT INTO marcas (id_deportista, id_especialidad, id_torneo, nombre_deportista, marca, intento)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    db.execute(query_marca, (id_deportista, id_especialidad, id_torneo, nombre_deportista, marca, intento))
    
    
# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    # archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # luego para cada linea generada como reporte:
    # grabar_linea(archivo, linea)
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")
    grabar_linea(archivo, "nombre especialidad - nombre torneo - nombre deportista - intento - marca")
    
    query_especialida_torneos = "SELECT * FROM especialidad_torneos"
    torneos = db.execute(query_especialida_torneos)
    for torneo in torneos: 
        tipo_ordenamiento = "ASC" if torneo.tipo == 'tiempo' else "DESC"
        query_marcas = f"""
            SELECT nombre_deportista, marca, intento 
            FROM marcas WHERE id_especialidad = %s AND id_torneo = %s
            ORDER BY marca {tipo_ordenamiento} 
            LIMIT 3
        """
        marcas = db.execute(query_marcas, (torneo.id_especialidad, torneo.id_torneo))
        for marca in marcas:
            grabar_linea(archivo, f"{torneo.nombre_especialidad} - {torneo.nombre_torneo} - {marca.nombre_deportista} - {marca.intento} - {marca.marca} ")

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    # Borrar la estructura de la base de datos
    db.execute("DROP KEYSPACE IF EXISTS tp3_ej06;")
    pass

# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
