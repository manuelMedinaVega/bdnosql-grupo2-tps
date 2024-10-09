import csv
import redis

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'tp2/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2/tp2_ej07.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = {
    'redisurl': 'localhost',
    'redispuerto': 6379
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
        if 0 == count % 10000:
            endbloque = time.time()
            tiempo = endbloque - startbloque
            #print(str(count) + " en " + str(tiempo) + " segundos")
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
    r = redis.Redis(conn["redisurl"], conn["redispuerto"], db=0, decode_responses=True)
    return r
    # crear db


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):    
    especialidad = fila['id_especialidad']
    score = float(fila['marca'])

    sorted_set_key = f"especialidad:{especialidad}"

    # Insertar en el SortedSet
    elemento = f"{fila['id_torneo']}:{fila['intento']}:{fila['id_deportista']}"
    db.zadd(sorted_set_key, {elemento: score})
# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")
    
    # Obtener todas las especialidades
    especialidades = db.keys("especialidad:*")
    
    for especialidad_key in especialidades:
        especialidad = especialidad_key.split(":")[1]
        
        # Asumo que las especialidades con id menor que 13 requieren una marca mayor para ser mejor
        if int(especialidad) < 13:
            # Obtener los 3 mejores (mayores marcas)
            podio = db.zrevrange(especialidad_key, 0, 2, withscores=True)
        else:
            # Obtener los 3 mejores (menores marcas)
            podio = db.zrange(especialidad_key, 0, 2, withscores=True)
        
        # Generar el reporte para la especialidad
        for posicion, (elemento, marca) in enumerate(podio, 1):
            torneo, intento, deportista = elemento.split(":")
            grabar_linea(archivo, f"Posición {posicion} - Especialidad: {especialidad}, Deportista: {deportista}, Torneo: {torneo}, Intento: {intento}, Marca: {marca}")


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
