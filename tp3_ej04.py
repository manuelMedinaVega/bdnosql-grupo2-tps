import csv
from cassandra.cluster import Cluster
import os

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'C:\\Users\\Kalou\\OneDrive\\Escritorio\\BasesDatosNoSQL\\TP3\\full_export.csv'

nombre_archivo_resultado_ejercicio= 'C:/Users/Kalou/OneDrive/Escritorio/BasesDatosNoSQL/TP3/tp3_ej04.txt'

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
    db.shutdown()
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
    cassandra_session.execute("""
                              CREATE KEYSPACE IF NOT EXISTS set_keyspace
                            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                                """)
    cassandra_session.set_keyspace("set_keyspace")
    cassandra_session.execute("""
                                CREATE TABLE IF NOT EXISTS deportista_especialidad2 (
                                    id_deportista INT,
                                    especialidad TEXT,
                                    torneo TEXT,
                                    intento INT,
                                    marca FLOAT,
                                    PRIMARY KEY ((id_deportista, especialidad), marca)
                                )

                              """)
    cassandra_session.execute("""
                                CREATE TABLE IF NOT EXISTS deportistas_unicos2 (
                                    id_deportista INT PRIMARY KEY,
                                    nombre TEXT
                                )
                            """)

    # Crear tabla para especialidades únicas
    cassandra_session.execute("""
                                CREATE TABLE IF NOT EXISTS especialidades_unicas2 (
                                    id_especialidad INT PRIMARY KEY,
                                    especialidad TEXT
                                )
                            """)
    
    # crear db
    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
# Inserta o actualiza las especialidades del tipo usando un set
def procesar_fila(db, fila):
    id_deportista = int(fila['id_deportista'])
    id_especialidad = int(fila['id_especialidad'])
    especialidad = fila['nombre_especialidad']
    torneo = fila['nombre_torneo']
    intento = int(fila['intento'])
    marca = float(fila['marca'])
    nombre = fila['nombre_deportista']

    query = """
    INSERT INTO deportista_especialidad2 (id_deportista, especialidad, torneo, intento, marca)
    VALUES (%s, %s, %s, %s, %s)
    """
    db.execute(query, (id_deportista, especialidad, torneo, intento, marca))
    
    query2 = """
    INSERT INTO deportistas_unicos2 (id_deportista, nombre)
    VALUES (%s, %s)
    """
    db.execute(query2, (id_deportista, nombre))
    
    query3 = """
    INSERT INTO especialidades_unicas2 (id_especialidad, especialidad)
    VALUES (%s, %s)
    """
    db.execute(query3, (id_especialidad, especialidad))
      
    pass

    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    # Primer archivo para el primer listado
    with open(nombre_archivo_resultado_ejercicio, "w", encoding="utf-8") as archivo:
         # Obtener todas las combinaciones de deportista y especialidad
        fila_cabecera = "id_deportista, nombre_deportista, Especialidad, Mejor marca, Mejor torneo, Mejor intento, Peor marca, Peor torneo, Peor intento"
        grabar_linea(archivo, fila_cabecera)
        # Obtener todos los deportistas únicos
        query_deportistas = "SELECT id_deportista FROM deportistas_unicos2"
        deportistas = db.execute(query_deportistas)

        for deportista in deportistas:
            id_deportista = deportista.id_deportista 
            
            query_especialidades = """
            SELECT especialidad FROM especialidades_unicas2
            """
            especialidades = db.execute(query_especialidades)

            for especialidad in especialidades:
                especialidad = especialidad.especialidad

                # Obtener la mejor marca (máxima)
                query_mejor = """
                SELECT torneo, intento, marca FROM deportista_especialidad2
                WHERE id_deportista = %s AND especialidad = %s
                ORDER BY marca DESC LIMIT 1
                """
                mejor_marca = db.execute(query_mejor, (id_deportista, especialidad)).one()

                # Obtener la peor marca (mínima)
                query_peor = """
                SELECT nombre_deportista, torneo, intento, marca FROM deportista_especialidad2
                WHERE id_deportista = %s AND especialidad = %s
                ORDER BY marca ASC LIMIT 1
                """
                peor_marca = db.execute(query_peor, (id_deportista, especialidad)).one()

                if mejor_marca and peor_marca:
                    # Formatear la línea para grabarla en el archivo
                    linea = (
                        f"{id_deportista}, {mejor_marca.nombre_deportista}, {mejor_marca.especialidad}, {mejor_marca.marca}, {mejor_marca.torneo}, {mejor_marca.intento}, "
                        f"{peor_marca.marca}, {peor_marca.torneo}, {peor_marca.intento}"
                    )
                    grabar_linea(archivo, linea)
    
    pass


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.shutdown()
    print("Conexión finalizada")
    pass
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
