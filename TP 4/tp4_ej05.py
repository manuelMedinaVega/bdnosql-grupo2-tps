import csv
from pymongo import MongoClient

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp4_ej05.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = {
    'mongourl': 'localhost',
    'mongopuerto': 27017
}


# Funcion que dada la configuracion y ubicacion del archivo, carga la base de datos, genera el reporte, y borra la
# base de datos
def ejecutar(file, conn):
    import time

    start = time.time()
    client = inicializar(conn)
    df_filas = csv.DictReader(open(file, "r", encoding="utf-8"))
    count = 0
    startbloque = time.time()
    for fila in df_filas:
        procesar_fila(client, fila)
        count += 1
        if 0 == count%100:
            endbloque = time.time()
            tiempo = endbloque-startbloque
            print(str(count) + " en " + str(tiempo) + " segundos")
            startbloque = time.time()
    generar_reporte(client)
    finalizar(client)
    end = time.time()
    print("tiempo total en segundos")
    print(end - start)


# Funcion que dado un archivo abierto y una linea, imprime por consola y guarda al final de archivo esa linea
def grabar_linea(archivo, linea):
    print(linea)
    archivo.write(str(linea) + '\n')


def inicializar(conn):
    r = MongoClient(conn["mongourl"], conn["mongopuerto"])
    # crear db
    return r


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(client, fila):
    db = client["TP4"]
    collection = db.segunda_parte    

    id = int(fila["id_deportista"])
    nombre = fila["nombre_deportista"]
    fecha_nacimiento = fila["fecha_nacimiento"]
    id_pais = int(fila["id_pais_deportista"])
    pais = fila["nombre_pais_deportista"] 
    id_especialidad = int(fila["id_especialidad"])
    nombre_especialidad = fila["nombre_especialidad"]
    id_tipo_especialidad = int(fila["id_tipo_especialidad"])
    nombre_tipo_especialidad = fila["nombre_tipo_especialidad"] 
    id_torneo = int(fila["id_torneo"])
    nombre_torneo = fila["nombre_torneo"]
    id_ciudad_torneo = int(fila["id_ciudad_torneo"])
    nombre_ciudad_torneo = fila["nombre_ciudad_torneo"] 
    id_pais_torneo = int(fila["id_pais_torneo"])
    nombre_pais_torneo = fila["nombre_pais_torneo"]
    intento = fila["intento"]
    marca = fila["marca"] 
    
    documento = {"_id": id,
                 "nombre": nombre,
                 "fecha_nac": fecha_nacimiento,
                 "id_pais": id_pais,
                 "pais": pais,
                 "id_especialidad": id_especialidad,
                 "nombre_especialidad": nombre_especialidad,
                 "id_tipo_especialidad": id_tipo_especialidad,
                 "nombre_tipo_especialidad": nombre_tipo_especialidad,
                 "id_torneo": id_torneo,
                 "nombre_torneo": nombre_torneo,
                 "id_ciudad_torneo": id_ciudad_torneo,
                 "nombre_ciudad_torneo": nombre_ciudad_torneo,
                 "id_pais_torneo": id_pais_torneo,
                 "nombre_pais_torneo": nombre_pais_torneo,
                 "intento": intento,
                 "marca": marca                 
                 }
												

    try:
        collection.insert_one(documento)
    except:
        pass
    pass
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(client):
    # No se genera el reporte acá, se debe utilizar una query utilizando un cliente de Mongo, como por ejemplo Robo3T
    # archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # luego para cada linea generada como reporte:
    # grabar_linea(archivo, linea)
    pass


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(client):
    # No se elimna la BD porque se utiliza en varios ejercicios
    # client.drop_database('some_collection')
    pass



# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
