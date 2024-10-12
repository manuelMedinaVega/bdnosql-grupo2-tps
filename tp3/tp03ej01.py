import csv
from cassandra.cluster import Cluster

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'tp2/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp3/tp3_ej01.txt'

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
        if 0 == count%1000:
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
    cassandra_session.set_keyspace("keyspace1")
    cassandra_session.execute("""
                              CREATE TABLE IF NOT EXISTS deportistas(
                              id INT PRIMARY KEY,
                              nombre TEXT)
                              """)
    # crear db
    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    id = int(fila["id_deportista"])
    nombre = fila["nombre_deportista"]

    query = """ 
    INSERT INTO deportistas (id, nombre)
    values (%s, %s)
    """

    db.execute(query, (id, nombre))
    
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    
    with open(nombre_archivo_resultado_ejercicio, 
              "w", encoding="utf-8") as archivo:

        query = """
                Select id, nombre from deportistas where id in (10, 20, 30)
                """

        filas = db.execute(query)

        fila_cabecera= "ID, NOMBRE"
        grabar_linea(archivo, fila_cabecera)
        for fila in filas:
            grabar_linea(archivo, f"{fila.id}, {fila.nombre}")

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.shutdown()
    print("Conexión finalizada")
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
