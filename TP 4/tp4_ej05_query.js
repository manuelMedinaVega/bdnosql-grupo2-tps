db.getCollection('segunda_parte').aggregate([{$group:{_id:{ id_tipo_especialidad: "$id_tipo_especialidad", 
    nombre_tipo_especialidad: "$nombre_tipo_especialidad" },nombre_especialidades:{$addToSet:"$nombre_especialidad"}}}]).pretty(); 
