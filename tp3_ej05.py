import csv
from cassandra.cluster import Cluster
import os

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'C:\\Users\\Kalou\\OneDrive\\Escritorio\\BasesDatosNoSQL\\TP3\\full_export.csv'

nombre_archivo_resultado_ejercicio= 'C:/Users/Kalou/OneDrive/Escritorio/BasesDatosNoSQL/TP3/tp3_ej05.txt'

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
                              CREATE KEYSPACE IF NOT EXISTS set_keyspace6
                            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                                """)
    cassandra_session.set_keyspace("set_keyspace6")
    cassandra_session.execute("""
                                CREATE TABLE IF NOT EXISTS especialidad_tipo_especialidad(
                                    id_especialidad INT,
                                    especialidad TEXT,
                                    nombre_tipo_especialidad TEXT,
                                    nombre_deportista TEXT,
                                    torneo TEXT,
                                    intento INT,
                                    marca FLOAT,
                                    PRIMARY KEY ((id_especialidad), marca)
                                )

                              """)
        # Crear tabla para especialidades únicas
    cassandra_session.execute("""
                                CREATE TABLE IF NOT EXISTS especialidades_unicas2 (
                                    id_especialidad INT PRIMARY KEY,
                                    especialidad TEXT,
                                    nombre_tipo_especialidad TEXT
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
    id_tipo_especialidad = int(fila['id_tipo_especialidad'])
    nombre_tipo_especialidad = fila['nombre_tipo_especialidad']
    especialidad = fila['nombre_especialidad']
    torneo = fila['nombre_torneo']
    intento = int(fila['intento'])
    marca = float(fila['marca'])
    nombre_deportista = fila['nombre_deportista']

    query = """
    INSERT INTO especialidad_tipo_especialidad (id_especialidad, nombre_deportista, especialidad, nombre_tipo_especialidad, torneo, intento, marca)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    db.execute(query, (id_especialidad,nombre_deportista, especialidad, nombre_tipo_especialidad, torneo, intento, marca))
        
    query3 = """
    INSERT INTO especialidades_unicas2 (id_especialidad, especialidad, nombre_tipo_especialidad )
    VALUES (%s, %s, %s)
    """
    db.execute(query3, (id_especialidad, especialidad, nombre_tipo_especialidad ))
      
      
    pass

    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    # Primer archivo para el primer listado
    with open(nombre_archivo_resultado_ejercicio, "w", encoding="utf-8") as archivo:
        # Cabecera del archivo
        fila_cabecera = "Nombre_tipo_Especialidad, Especialidad, Nombre_Deportista, Marca, Torneo, Intento"
        grabar_linea(archivo, fila_cabecera)

        # Obtener todas las especialidades
        query_especialidades = """
        SELECT id_especialidad , nombre_tipo_especialidad FROM especialidades_unicas2
        """
        especialidades = db.execute(query_especialidades)

        for especialidad in especialidades:
            id_especialidad = especialidad.id_especialidad
            tipo_especialidad = especialidad.nombre_tipo_especialidad

            tipo_ordenamiento = "ASC" if tipo_especialidad == 'tiempo' else "DESC"
            query_top3 = f"""
                SELECT nombre_tipo_especialidad, especialidad, nombre_deportista, marca, torneo, intento 
                FROM especialidad_tipo_especialidad 
                WHERE id_especialidad = %s 
                ORDER BY marca {tipo_ordenamiento} 
                LIMIT 3
            """
            
            top3_rows = db.execute(query_top3, (id_especialidad,))

            # Escribimos las filas del podio en el archivo
            for row in top3_rows:
                linea = (f"{row.nombre_tipo_especialidad}, {row.especialidad}, {row.nombre_deportista}, {row.marca}, {row.torneo}, {row.intento}")
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
