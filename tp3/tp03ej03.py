import csv
from cassandra.cluster import Cluster

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'tp2/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp3/tp3_ej03.txt'
nombre_archivo_2 = 'tp3/tp3_ej03B.txt'


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
        if 0 == count%10000:
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
                              CREATE TABLE if not exists especialidades(
                              id_tipo_especialidad INT,
                              nombre_tipo_especialidad TEXT,
                              id_especialidad INT,
                              especialidad TEXT,
                              primary key (id_tipo_especialidad, id_especialidad)); """) 
    
    cassandra_session.execute("""
                              CREATE TABLE IF NOT EXISTS marcas2 (
                              id_deportista INT,
                                especialidad TEXT,                                
                                id_torneo INT,
                                intento INT,
                                marca INT,
                                PRIMARY KEY ((id_deportista, especialidad, id_torneo, intento))
                            ); """)
    
      
    
        
    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    # id_tipo = int(fila["id_tipo_especialidad"])
    # nombre_tipo = fila["nombre_tipo_especialidad"]
    # id_especialidad = int(fila["id_especialidad"])
    # especialidad = fila["nombre_especialidad"]


    # query = """ 
    # INSERT INTO especialidades (id_tipo_especialidad, nombre_tipo_especialidad, id_especialidad, especialidad)
    # values (%s, %s, %s, %s)    """

    id = int(fila["id_deportista"])
    especialidad = fila["nombre_especialidad"]
    torneo = int(fila["id_torneo"])
    intento = int(fila["intento"])
    marca = int(fila["marca"])


    query = """ 
    INSERT INTO marcas2 (id_deportista, especialidad, id_torneo, 
    intento, marca)
    values (%s, %s, %s, %s, %s)
    """   

    db.execute(query, (id, especialidad, torneo, intento, marca))

    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    
    with open(nombre_archivo_resultado_ejercicio, 
              "w", encoding="utf-8") as archivo:

        query = """
                Select id_tipo_especialidad, nombre_tipo_especialidad, 
                id_especialidad, especialidad from especialidades
                """

        filas = db.execute(query)      

        fila_cabecera= "ID_TIPO, NOMBRE_TIPO, ESPECIALIDADES"
        grabar_linea(archivo, fila_cabecera)
        for fila in filas:          
                query2 = f"""
                Select id_tipo_especialidad, especialidad from especialidades 
                where id_tipo_especialidad = {fila.id_tipo_especialidad} 
                """

                filas2 = db.execute(query2)
                lista_espec = []
                
                for fila2 in filas2:
                    lista_espec.append(fila2.especialidad)
                                        
                    
                grabar_linea(archivo, f"{fila.id_tipo_especialidad}, {fila.nombre_tipo_especialidad}, {lista_espec}")
        
            
    with open(nombre_archivo_2, 
              "w", encoding="utf-8") as archivo:
        fila_cabecera= "NOMBRE_TIPO, CANTIDAD_MARCAS"
        grabar_linea(archivo, fila_cabecera)
        for id_tipo in range(1,5):
            query = f"""
                    Select id_tipo_especialidad, nombre_tipo_especialidad, 
                    id_especialidad, especialidad from especialidades
                    where id_tipo_especialidad = {id_tipo} and id_especialidad > 0
                    """

            filas = db.execute(query)  
            
            

                              
            contador_marcas = 0
            for fila in filas:
                        
                    # segundo listado. 
                    # Traigo de la tabla marcas las filas de esa especialidad
                    query3 =f"""
                                Select id_deportista, especialidad, marca from marcas2 
                                where especialidad = '{fila.especialidad}' and id_deportista > 0
                                and id_torneo > 0 and intento > 0
                                ALLOW FILTERING
                                """
                    marcas = list(db.execute(query3)) 
                    contador_marcas += len(marcas)
                        
            grabar_linea(archivo, f"{fila.nombre_tipo_especialidad}, {contador_marcas}")
                    

            
# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.shutdown()
    print("Conexión finalizada")
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
