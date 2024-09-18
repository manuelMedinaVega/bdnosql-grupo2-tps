import csv
import redis

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = r'C:\Users\Kalou\OneDrive\Escritorio\BasesDatosNoSQL\full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2_ej03.txt'

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
    db.flushdb()
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
    key = f"deportista_{fila['id_deportista']}:{fila['nombre_especialidad']}:{fila['nombre_tipo_especialidad']}"
    torneo = fila['nombre_torneo']
    intento = fila['intento']
    nombre_tipo_especialidad = fila['nombre_tipo_especialidad']
    valor = f"{torneo}:{intento}"
    db.zadd(key, {valor: score})
   
    
# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # luego para cada linea generada como reporte:
    
        # luego para cada linea generada como reporte:
    # Función para obtener la mejor y peor marca de un deportista en una especialidad
    # Función para obtener la mejor y peor marca de un deportista en una especialidad


    # Obtener todas las claves que siguen el patrón 'deportista:{id_deportista}:{especialidad}'
    claves = db.keys('deportista*')
    
    # Recorrer todas las claves
    for clave in claves:
        
        # Extraer id_deportista y especialidad de la clave
        partes = clave.split(':')
        id_deportista = partes[0]
        especialidad = partes[1]
        tipo_especialidad = partes[2]
        
        if tipo_especialidad == 'tiempo':
             # Obtener la peor marca (mínima puntuación)
            mejor_marca = db.zrange(clave, 0, 0, withscores=True)
            
            # Obtener la mejor marca (máxima puntuación)
            peor_marca = db.zrevrange(clave, 0, 0, withscores=True)
            
            if peor_marca and mejor_marca:
                peor_valor, peor_score = peor_marca[0]
                mejor_valor, mejor_score = mejor_marca[0]
            
        # Dividir el valor en torneo e intento, asegurando que solo haya un split
                peor_torneo, peor_intento = peor_valor.split(':', 1)
                mejor_torneo, mejor_intento = mejor_valor.split(':', 1)
                
                linea = (
                    f"Deportista: {id_deportista}, "
                    f"Especialidad: {especialidad}, "
                    f"  Mejor marca: {mejor_score} en {mejor_torneo}, intento {mejor_intento}"
                    f"  Peor marca: {peor_score} en {peor_torneo}, intento {peor_intento}"
                )
                
                grabar_linea(archivo, linea)
            
        else:   

            # Obtener la peor marca (mínima puntuación)
            peor_marca = db.zrange(clave, 0, 0, withscores=True)
            
            # Obtener la mejor marca (máxima puntuación)
            mejor_marca = db.zrevrange(clave, 0, 0, withscores=True)
            
            if peor_marca and mejor_marca:
                peor_valor, peor_score = peor_marca[0]
                mejor_valor, mejor_score = mejor_marca[0]
            
        # Dividir el valor en torneo e intento, asegurando que solo haya un split
                peor_torneo, peor_intento = peor_valor.split(':', 1)
                mejor_torneo, mejor_intento = mejor_valor.split(':', 1)
                
                linea = (
                    f"Deportista: {id_deportista}, "
                    f"Especialidad: {especialidad}, "
                    f"  Mejor marca: {mejor_score} en {mejor_torneo}, intento {mejor_intento}"
                    f"  Peor marca: {peor_score} en {peor_torneo}, intento {peor_intento}"
                )
                
                grabar_linea(archivo, linea)
            
# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)