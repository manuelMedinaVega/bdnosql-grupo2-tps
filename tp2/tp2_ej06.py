import csv
import redis
import os

# Ubicacion del archivo CSV con el contenido provisto por la catedra
directorio_actual = os.path.dirname(os.path.abspath(__file__))
archivo_entrada =  os.path.join(directorio_actual, '..', 'datos', 'full_export.csv')
nombre_archivo_resultado_ejercicio = os.path.join(directorio_actual, 'tp2_ej06.txt')

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
    
    
    sorted_set_key = f"ranking:{fila['id_especialidad']}"

    marca = fila['marca']
    nombre = fila['nombre_deportista']            
    torneo = fila['id_torneo']
    intento = fila['intento']            

    elemento_zset = f"{torneo}:{intento}:{nombre}"
    score_marca = float(marca)

    db.zadd(sorted_set_key, {elemento_zset: score_marca})

    
        

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")      

    for id in range(1,21):
            print(f"sset: {id}")

            grabar_linea(archivo, f"Especialidad: {id}")

            sorted_set_key = f"ranking:{id}"
            if id < 13:                
                podio = db.zrange(sorted_set_key, 0, 2, withscores=True)
            else:
                podio = db.zrevrange(sorted_set_key, 0, 2, withscores=True)

            for posicion, (elemento, score) in enumerate(podio, start=1):
                partes_key = elemento.split(":")
                intento = partes_key[1]
                nombre = partes_key[2] 
                grabar_linea(archivo, f"{posicion}. Intento: {intento}, nombre: {nombre} - Marca: {score}")


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
