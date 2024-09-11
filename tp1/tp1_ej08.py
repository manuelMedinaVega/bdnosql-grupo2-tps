import csv

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej08.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = []


# Funcion que dada la configuracion y ubicacion del archivo, carga la base de datos, genera el reporte, y borra la
# base de datos
def ejecutar(file, conn):
    db = inicializar(conn)
    df_filas = csv.DictReader(open(file, "r", encoding="utf-8"))
    for fila in df_filas:
        procesar_fila(db, fila)
    generar_reporte(db)
    finalizar(db)


# Funcion que dado un archivo abierto y una linea, imprime por consola y guarda al final de archivo esa linea
def grabar_linea(archivo, linea):
    print(linea)
    archivo.write(linea+'\n')


# Funcion para poner el codigo que cree las estructuras a usarse en el este ejercicio
# Debe ser implementada por el alumno
def inicializar(conn):
    return set()
    # crear db


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(database, fila):
    
    database.add((fila["nombre_torneo"], fila['nombre_especialidad'], fila['nombre_deportista'], fila['marca'], fila['nombre_tipo_especialidad']))
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(database):
    
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8')
    
    torneos = {}
    
    for fila in database:
        if fila[0] not in torneos: #torneos
            torneos[fila[0]] = {}
        if fila[1] not in torneos[fila[0]]: #especialidades
            torneos[fila[0]][fila[1]] = {'tipo': fila[4], 'resultados': []}
        torneos[fila[0]][fila[1]]['resultados'].append((fila[2], float(fila[3])))

    for torneo, especialidades in torneos.items(): 
        especialidades_ordenadas = sorted(especialidades.items())
        for especialidad, data in especialidades_ordenadas:
            if data['tipo'] == 'tiempo': 
                resultados_ordenados = sorted(data['resultados'], key = lambda x: x[1])
            else:
                resultados_ordenados = sorted(data['resultados'], key = lambda x: x[1], reverse=True)
            
            podio = resultados_ordenados[:3]  
            for i, resultado in enumerate(podio, start = 0):
                deportista, marca = resultado
                grabar_linea(archivo, f"{torneo} - {especialidad} - {deportista} - {marca}")


# Funcion para el borrado de estructuras generadas para este ejercicio
# Debe ser implementada por el alumno
def finalizar(database):
    database.clear()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
