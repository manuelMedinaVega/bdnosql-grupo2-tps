import csv
import redis

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'tp2/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2/tp2_ej03.txt'

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
        if 0 == count % 100:
            endbloque = time.time()
            tiempo = endbloque - startbloque
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
    r = redis.Redis(conn["redisurl"], conn["redispuerto"], db=0, decode_responses=True)
    return r
    # crear db


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    id_tipo_especialidad = fila['id_tipo_especialidad']
    tipo_especialidad = fila['nombre_tipo_especialidad']
    nombre_especialidad = fila['nombre_especialidad']

    db.hset(f"especialidades", id_tipo_especialidad, tipo_especialidad)
    db.sadd(f"especialidades_tipo:{id_tipo_especialidad}", nombre_especialidad)


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    tipos_especialidad = db.hgetall("especialidades")

    with open(nombre_archivo_resultado_ejercicio, 'w') as archivo:
        for id_tipo, nombre_tipo in tipos_especialidad.items():
            cantidad_especialidades = db.scard(f"especialidades_tipo:{id_tipo}")

            linea = (
                f"ID Tipo Especialidad: {id_tipo}, "
                f"Nombre: {nombre_tipo}, "
                f"Cantidad de Especialidades: {cantidad_especialidades}"
            )
            grabar_linea(archivo, linea)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
