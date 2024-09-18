import csv
import redis

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'tp2/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2/tp2_ej05.txt'

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
    
    # insertar elemento en entidad para el ejercicio actual
    # usé hset de esta forma ya que con mapping me devolvía error
    if not db.exists(f"{fila['id_especialidad']}:{fila['id_torneo']}:{fila['intento']}:{fila['id_deportista']}"):
        db.hset(f"{fila['id_especialidad']}:{fila['id_torneo']}:{fila['intento']}:{fila['id_deportista']}", 
                'nombre', fila['nombre_deportista'])
        db.hset(f"{fila['id_especialidad']}:{fila['id_torneo']}:{fila['intento']}:{fila['id_deportista']}", 
                'marca', fila['marca'])
        

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")
    # luego para cada linea generada como reporte:
    
    # Iniciar el cursor de SCAN
    cursor = '0'
    deportistas_especialidades = {}

    while cursor != 0:
        cursor, keys = db.scan(cursor=cursor, match="*:*:*:*")  # Patrón para todas las claves con 4 partes
        for key in keys:
            # Separar la clave en sus componentes
            partes = key.split(':')
            if len(partes) == 4:  # Asegurarse de que la clave tiene el formato esperado
                id_especialidad = partes[0]
                id_deportista = partes[3]
                
                # Si el deportista no está en el diccionario, inicializar su lista de especialidades
                if id_deportista not in deportistas_especialidades:
                    deportistas_especialidades[id_deportista] = []

                # Agregar la especialidad si no está ya en la lista
                if id_especialidad not in deportistas_especialidades[id_deportista]:              
                        
                    deportistas_especialidades[id_deportista].append(id_especialidad)


    for deportista, especialidades in deportistas_especialidades.items():
        for especialidad in especialidades:
            claves = db.keys(f"{especialidad}:*:*:{deportista}") 
            sorted_set_key = f"{especialidad}:{deportista}"
            
            for clave in claves:

                marca = db.hget(clave, 'marca')
                
                partes_key = clave.split(":")
                torneo = partes_key[1]
                intento = partes_key[2]              

                elemento_zset = f"{torneo}:{intento}"
                score_marca = float(marca)

                db.zadd(sorted_set_key, {elemento_zset: score_marca})      

            
                            
            podio1 = db.zrange(sorted_set_key, 0, 0, withscores=True)            
            podio2 = db.zrevrange(sorted_set_key, 0, 0, withscores=True)
            elemento1, puntaje1 = podio1[0]
            elemento2, puntaje2 = podio2[0]

            partes_key = elemento1.split(":")
            intento1 = partes_key[1]
            torneo1 = partes_key[0] 

            partes_key2 = elemento2.split(":")
            intento2 = partes_key2[1]
            torneo2 = partes_key2[0] 

            if int(especialidad) < 13:         
        
                grabar_linea(archivo, f"Mejor marca, deportista: {deportista}, especialidad: {especialidad}, torneo: {torneo2}, Intento: {intento2} - Marca: {puntaje2}")
                grabar_linea(archivo, f"Peor marca, deportista: {deportista}, especialidad: {especialidad}, torneo: {torneo1}, Intento: {intento1} - Marca: {puntaje1}")

            else:
                grabar_linea(archivo, f"Mejor marca, deportista: {deportista}, especialidad: {especialidad}, torneo: {torneo1}, Intento: {intento1} - Marca: {puntaje1}")
                grabar_linea(archivo, f"Peor marca, deportista: {deportista}, especialidad: {especialidad}, torneo: {torneo2}, Intento: {intento2} - Marca: {puntaje2}")


    

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
