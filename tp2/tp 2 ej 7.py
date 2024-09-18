import csv
import redis

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = r'C:\Users\Kalou\OneDrive\Escritorio\BasesDatosNoSQL\full_export.csv'
nombre_archivo_resultado_ejercicio = r'C:\Users\Kalou\OneDrive\Escritorio\BasesDatosNoSQL\tp2_ej07.txt'

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
    # insertar elemento en entidad para el ejercicio actual

    score = float(fila['marca'])
    key = f"torneo:{fila['nombre_torneo']}:{fila['nombre_especialidad']}:{fila['nombre_tipo_especialidad']}"
    intento = fila['intento']
    nombre_deportista = fila['nombre_deportista']
    valor = f"{intento}:{nombre_deportista}"
    db.zadd(key, {valor: score})
   
    
# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")
    # luego para cada linea generada como reporte:
    
        # luego para cada linea generada como reporte:
    # Función para obtener la mejor y peor marca de un deportista en una especialidad
    # Función para obtener la mejor y peor marca de un deportista en una especialidad


    # Obtener todas las claves que siguen el patrón 'deportista:{id_deportista}:{especialidad}'
    claves = db.keys('torneo*')
    
    for clave in claves:
            partes = clave.split(':')
            torneo= partes[1]
            especialidad = partes[2]
            tipo_especialidad = partes[3]
            
            if tipo_especialidad == 'tiempo':
                top_3 = db.zrange(clave, 0, 2, withscores=True)
            else:
                top_3 = db.zrevrange(clave, 0, 2, withscores=True)
            
            for i, (valor, score) in enumerate(top_3):
                intento, nombre_deportista = valor.split(':')
                posicion = i + 1
                linea = (
                    f"Torneo: {torneo}, "
                    f"Especialidad: {especialidad} - Posición {posicion}: "
                    f"Deportista: {nombre_deportista}, "
                    f"Marca: {score}, "
                     f"Intento: {intento}"
                )
                grabar_linea(archivo, linea)
                
# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)