import csv
from cassandra.cluster import Cluster

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = './tp3/full_export.csv'
nombre_archivo_resultado_ejercicio = './tp3/tp3_ej03.txt'

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
    cassandra_session.execute('DROP TABLE IF EXISTS ej03')
    cassandra_session.execute('''
        CREATE TABLE tp3.ej03 (
            nombre_tipo_especialidad TEXT,
            id_deportista INT,
            nombre_deportista TEXT,
            nombres_especialidades set<TEXT>, 
            PRIMARY KEY (nombre_tipo_especialidad, id_deportista)
        );
    ''')

    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    # insertar elemento en entidad para el ejercicio actual
    nombre_tipo_especialidad = fila['nombre_tipo_especialidad']
    id_deportista = int(fila['id_deportista'])
    nombre_deportista = fila['nombre_deportista']
    nombre_especialidad = fila['nombre_especialidad']
    
    query = '''
        UPDATE tp3.ej03 
        SET nombre_deportista = %s, nombres_especialidades = nombres_especialidades + {%s}
        WHERE nombre_tipo_especialidad = %s AND id_deportista = %s;
    '''
    
    db.execute(query, (nombre_deportista, nombre_especialidad, nombre_tipo_especialidad, id_deportista))

    pass

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    # archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # luego para cada linea generada como reporte:
    # grabar_linea(archivo, linea)
    
    query_esportistas = '''
        SELECT nombre_tipo_especialidad, id_deportista, nombre_deportista, nombres_especialidades 
        FROM tp3.ej03
    '''
    
    query_conteo = '''
        SELECT nombre_tipo_especialidad, COUNT(*) AS cantidad
        FROM tp3.ej03
        GROUP BY nombre_tipo_especialidad;
    '''
    
    with open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8') as archivo:
        rows_esportistas = db.execute(query_esportistas)
        archivo.write("NombreTipoEspecialidade, IdDeportista, NombreDeportista, NombresEspecialidades\n")
        for row in rows_esportistas:
            grabar_linea(archivo, (row.nombre_tipo_especialidad + ", " + str(row.id_deportista) + ", " + row.nombre_deportista + ", " + ", ".join(row.nombres_especialidades)))

        
        archivo.write("\nNombreTipoEspecialidade, Cantidad\n")
        rows_conteo = db.execute(query_conteo)
        for row in rows_conteo:
            grabar_linea(archivo, (row.nombre_tipo_especialidad + " = " + str(row.cantidad)))


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    pass
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
