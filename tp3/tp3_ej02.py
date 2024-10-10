import csv
from cassandra.cluster import Cluster

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = './tp3/full_export.csv'
nombre_archivo_resultado_ejercicio = './tp3/tp3_ej02.txt'

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
    
    cassandra_session.execute("CREATE KEYSPACE IF NOT EXISTS tp3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    cassandra_session.set_keyspace('tp3')
    
    # crear db
    cassandra_session.execute('DROP TABLE IF EXISTS ej02')
    cassandra_session.execute('''
        CREATE TABLE tp3.ej02 (
            id_deportista INT, 
            nombre_deportista TEXT, 
            fecha_nacimiento DATE, 
            id_pais_deportista INT, 
            nombre_pais_deportista TEXT, 
            id_especialidad INT, 
            nombre_especialidad TEXT,
            PRIMARY KEY (id_deportista, nombre_especialidad)
        );
    ''')

    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    # insertar elemento en entidad para el ejercicio actual
    id_deportista = int(fila['id_deportista'])
    nombre_deportista = fila['nombre_deportista'] 
    fecha_nacimiento = (fila['fecha_nacimiento'])
    id_pais_deportista = int(fila['id_pais_deportista'])
    nombre_pais_deportista = fila['nombre_pais_deportista']
    id_especialidad = int(fila['id_especialidad'])
    nombre_especialidad = fila['nombre_especialidad']
    
    # Montar a query de inserção
    query = f'''
                INSERT INTO ej02 (id_deportista, nombre_deportista, 
                fecha_nacimiento, id_pais_deportista, nombre_pais_deportista, 
                id_especialidad, nombre_especialidad)
                values (%s, %s, %s, %s, %s, %s, %s);
            '''
    
    # Executar a query com os dados extraídos
    db.execute(query, (id_deportista, nombre_deportista, fecha_nacimiento, id_pais_deportista,
                        nombre_pais_deportista, id_especialidad, nombre_especialidad))
    pass

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    # archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # luego para cada linea generada como reporte:
    # grabar_linea(archivo, linea)
    
    query = "SELECT * FROM ej02 where id_deportista IN (10, 20, 30);"
    rows = db.execute(query)

    with open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8') as archivo:
        for row in rows:
            grabar_linea(archivo, (row.id_deportista, row.nombre_deportista, 
                                   row.fecha_nacimiento, row.id_pais_deportista,
                                   row.nombre_pais_deportista, row.id_especialidad,
                                   row.nombre_especialidad))
    pass


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    pass
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
