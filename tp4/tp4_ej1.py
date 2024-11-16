import csv
from pymongo import MongoClient as MC

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'tp2/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp4_ej1.txt'

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
        if 0 == count%10000:
            endbloque = time.time()
            tiempo = endbloque-startbloque
            print(str(count) + " en " + str(tiempo) + " segundos")
            startbloque = time.time()
    #generar_reporte(client)
    finalizar(client)
    end = time.time()
    print("tiempo total en segundos")
    print(end - start)


# Funcion que dado un archivo abierto y una linea, imprime por consola y guarda al final de archivo esa linea
def grabar_linea(archivo, linea):
    print(linea)
    archivo.write(str(linea) + '\n')


def inicializar(conn):
    r = MC(conn["mongourl"], conn["mongopuerto"])
    
    # crear db
    return r


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(client, fila):
    db = client["TP4"]
    collection = db.primera_parte    

    id = int(fila["id_deportista"])
    nombre = fila["nombre_deportista"]
    fecha_nacimiento = fila["fecha_nacimiento"]
    id_pais = int(fila["id_pais_deportista"])
    pais = fila["nombre_pais_deportista"] 
    
    documento = {"_id": id,
                 "nombre": nombre,
                 "fecha_nac": fecha_nacimiento,
                 "id_pais": id_pais,
                 "pais": pais}
    
    try:
        collection.insert_one(documento)
    except:
        pass
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
# def generar_reporte(client):
#     # No se genera el reporte acá, se debe utilizar una query utilizando un cliente de Mongo, como por ejemplo Robo3T
#     # archivo = open(nombre_archivo_resultado_ejercicio, 'w')
#     # luego para cada linea generada como reporte:
#     # grabar_linea(archivo, linea)

#     db = client["TP4"]
#     collection = db.ejercicio_1

#     with open(nombre_archivo_resultado_ejercicio, 
#               "w", encoding="utf-8") as archivo:
        
    
#         filas = collection.find({"id_deportista": {"$in": [10,20,30]}}, 
#                                 {"id_deportista": 1, "nombre": 1, "_id": 0})
        

#         fila_cabecera= "ID, NOMBRE"
#         grabar_linea(archivo, fila_cabecera)
#         for fila in filas:
#             grabar_linea(archivo, f"{fila.id_deportista}, {fila.nombre}")


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(client):
    # No se elimna la BD porque se utiliza en varios ejercicios
    # client.drop_database('some_collection')
    pass



# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
