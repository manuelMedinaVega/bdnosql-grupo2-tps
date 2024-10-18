import csv
from cassandra.cluster import Cluster
import os

# Ubicacion del archivo CSV con el contenido provisto por la catedra
directorio_actual = os.path.dirname(os.path.abspath(__file__))
archivo_entrada =  os.path.join(directorio_actual, '..', 'datos', 'full_export.csv')
nombre_archivo_resultado_ejercicio = os.path.join(directorio_actual, 'tp3_ej04.txt')

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
        CREATE KEYSPACE IF NOT EXISTS tp3_ej04 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    cassandra_session.set_keyspace('tp3_ej04')
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS marcas (
            id_deportista INT,
            id_especialidad INT,
            nombre_deportista TEXT,
            nombre_especialidad TEXT,
            nombre_tipo_especialidad TEXT,
            mejor_marca INT,
            peor_marca INT,
            nombre_torneo_mejor_marca TEXT,
            nombre_torneo_peor_marca TEXT,
            intento_mejor_marca INT,
            intento_peor_marca INT,
            PRIMARY KEY (id_deportista, id_especialidad)
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
    nombre_deportista = fila['nombre_deportista']
    nombre_especialidad = fila['nombre_especialidad']
    nombre_tipo_especialidad = fila['nombre_tipo_especialidad']
    nueva_mejor_marca = int(fila['marca'])
    nueva_peor_marca = int(fila['marca'])
    nombre_torneo_mejor_marca = fila['nombre_torneo']
    nombre_torneo_peor_marca = fila['nombre_torneo']
    intento_mejor_marca = int(fila['intento'])
    intento_peor_marca = int(fila['intento'])
    
    query_marcas = """
        SELECT mejor_marca, peor_marca FROM marcas WHERE id_deportista = %s AND id_especialidad = %s
    """
    result = db.execute(query_marcas, (id_deportista, id_especialidad)).one()
    #print(result)
    if result:
        mejor_marca, peor_marca, update_mejor, update_peor = result.mejor_marca, result.peor_marca, False, False
        
        if(nombre_tipo_especialidad == 'tiempo'):
            if nueva_mejor_marca < mejor_marca:
                update_mejor = True
            if nueva_peor_marca > peor_marca:
                update_peor = True
        else:
            if nueva_mejor_marca > mejor_marca:
                update_mejor = True
            if nueva_peor_marca < peor_marca:
                update_peor = True
                
        if update_mejor:
            query_update = """
                UPDATE marcas 
                SET mejor_marca = %s, intento_mejor_marca = %s, nombre_torneo_mejor_marca = %s
                WHERE id_deportista = %s AND id_especialidad = %s
            """
            db.execute(query_update, (nueva_mejor_marca, intento_mejor_marca, nombre_torneo_mejor_marca, id_deportista, id_especialidad))
        
        if update_peor:
            query_update = """
                UPDATE marcas 
                SET peor_marca = %s, intento_peor_marca = %s, nombre_torneo_peor_marca = %s
                WHERE id_deportista = %s AND id_especialidad = %s
            """
            db.execute(query_update, (nueva_peor_marca, intento_peor_marca, nombre_torneo_peor_marca, id_deportista, id_especialidad))
    
    else:
        query = """
            INSERT INTO marcas (id_deportista, id_especialidad, nombre_deportista, nombre_especialidad, 
                                nombre_tipo_especialidad, mejor_marca, peor_marca, nombre_torneo_mejor_marca, 
                                nombre_torneo_peor_marca, intento_mejor_marca, intento_peor_marca)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        db.execute(query, (id_deportista, id_especialidad, nombre_deportista, nombre_especialidad, 
                           nombre_tipo_especialidad, nueva_mejor_marca, nueva_peor_marca, nombre_torneo_mejor_marca, 
                           nombre_torneo_peor_marca, intento_mejor_marca, intento_peor_marca))
    
    
# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    # archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # luego para cada linea generada como reporte:
    # grabar_linea(archivo, linea)
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")
    grabar_linea(archivo, "nombre deportista - nombre especialidad - torneo mejor marca - intento mejor marca - mejor marca - torneo peor marca - intento peor marca - peor marca")
    
    query_marcas = """
        SELECT nombre_deportista, nombre_especialidad, nombre_torneo_mejor_marca, intento_mejor_marca, mejor_marca, 
            nombre_torneo_peor_marca, intento_peor_marca, peor_marca FROM marcas
    """
    marcas = db.execute(query_marcas)
    for marca in marcas: 
        grabar_linea(archivo, f"{marca.nombre_deportista} - {marca.nombre_especialidad} - {marca.nombre_torneo_mejor_marca} - {marca.intento_mejor_marca} - {marca.mejor_marca} - {marca.nombre_torneo_peor_marca} - {marca.intento_peor_marca} - {marca.peor_marca}")

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    # Borrar la estructura de la base de datos
    db.execute("DROP KEYSPACE IF EXISTS tp3_ej04;")
    pass

# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
