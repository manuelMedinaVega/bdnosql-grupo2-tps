import csv



# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej07.txt'

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
    database.add((fila['id_tipo_especialidad'], fila['nombre_especialidad'], fila['nombre_torneo'], fila['nombre_deportista'], fila['marca']))
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(database):
    especialidades_torneos = {}
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8')


    # Paso 1: obtengo las claves
    for fila in database:
        id_esp, especialidad, torneo, deportista, marca = fila

        # Usar una tupla (especialidad, torneo) como clave
        clave = (especialidad)
        
        # Verificar si la clave ya existe en el diccionario
        if clave not in especialidades_torneos:
            especialidades_torneos[clave] = []
            
    # Paso 2: para cada clave armo el podio y guardo los resultados
    for clave in especialidades_torneos:
        especialidad_actual = clave
        
        grabar_linea(archivo,f"{especialidad_actual}")

        resultados_filtrados = []

        for fila in database:
            id_esp, especialidad, torneo, deportista, marca = fila
             
            if especialidad == especialidad_actual:    
                           
                resultados_filtrados.append((id_esp, especialidad, torneo, deportista, float(marca)))

        # Ordenar por la marca (de menor a mayor) y obtener los primeros 3
        if id_esp != "1":
            resultados_filtrados.sort(key=lambda x: x[4], reverse=True)  # x[3] es la marca
        else:
            resultados_filtrados.sort(key=lambda x: x[4]) 

        # Quedarse con los primeros 3
        podio = resultados_filtrados[:3]

        # Imprimir los resultados del podio
        for id_esp, especialidad, torneo, deportista, marca in podio:
            print(f"{especialidad}, {torneo}, {deportista}")
            grabar_linea(archivo,f"{torneo}, {especialidad}, {deportista}, {marca}")
            # grabar_linea(archivo, linea)
                

        

    

# Funcion para el borrado de estructuras generadas para este ejercicio
# Debe ser implementada por el alumno
def finalizar(database):
    database.clear()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
