db.getCollection('segunda_parte').aggregate([{$group:{_id:"$nombre_tipo_especialidad", cantidad:{$sum:1}}},{ $sort: { cantidad: -1 } }]); 
