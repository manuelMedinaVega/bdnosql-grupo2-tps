import csv
from cassandra.cluster import Cluster

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = './tp3/full_export.csv'
nombre_archivo_resultado_ejercicio = './tp3/tp3_ej04.txt'

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
    cassandra_session.execute('DROP TABLE IF EXISTS ej04')
    cassandra_session.execute('''
        CREATE TABLE tp3.ej04 (
            nombre_tipo_especialidad TEXT,
            id_deportista INT,
            nombre_deportista TEXT,
            nombres_especialidades set<TEXT>,
            nombre_torneo TEXT,
            intento INT,
            marca DOUBLE,
            PRIMARY KEY ((id_deportista, nombre_tipo_especialidad), marca)
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
    torneo = fila['nombre_torneo']
    intento = int(fila['intento'])
    marca = float(fila['marca'])

    query = '''
        INSERT INTO tp3.ej04 (nombre_tipo_especialidad, id_deportista, nombre_deportista, nombres_especialidades, 
                              nombre_torneo, intento, marca)
        VALUES (%s, %s, %s, {%s}, %s, %s, %s)
    '''
    
    db.execute(query, (nombre_tipo_especialidad, id_deportista, nombre_deportista, nombre_especialidad, torneo, intento, marca))

    pass

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    # archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # luego para cada linea generada como reporte:
    # grabar_linea(archivo, linea)
    
    # Função que realiza o ou os queries que resolvem o exercício, utilizando a base de dados.
    query = '''
        SELECT id_deportista, nombre_deportista, nombre_tipo_especialidad, nombre_torneo, 
               MAX(marca) AS mejor_marca, MIN(marca) AS peor_marca, intento
        FROM tp3.ej04
        GROUP BY id_deportista, nombre_tipo_especialidad
    '''
    
    # Abre o arquivo para escrever o resultado
    with open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8') as archivo:
        deportistas = db.execute(query)
        for deportista in deportistas:
            linea = f"ID: {deportista.id_deportista} - Deportista: {deportista.nombre_deportista}, " \
                    f"Especialidad: {deportista.nombre_tipo_especialidad}, " \
                    f"Torneo: {deportista.nombre_torneo}, " \
                    f"Mejor Marca: {deportista.mejor_marca}, " \
                    f"Peor Marca: {deportista.peor_marca}, " \
                    f"Intento: {deportista.intento}"
            grabar_linea(archivo, linea)


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    pass
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
