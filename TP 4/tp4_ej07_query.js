db.getCollection('segunda_parte').aggregate([
  {
    $group: {
      _id: {
        deportista: "$id",
        nombre_deportista: "$nombre",
        especialidad: "$nombre_especialidad",
        tipo_especialidad: "$nombre_tipo_especialidad"
      },
      marcas: { $push: "$marca" }
    }
  },
   {
    $project: {
      deportista: "$_id.nombre_deportista",
      especialidad: "$_id.especialidad",
      mejor_marca: {
        $cond: {
          if: { $eq: ["$_id.tipo_especialidad", "tiempo"] },
          then: { $min: "$marcas" },
          else: { $max: "$marcas" }
        }
      },
      peor_marca: {
        $cond: {
          if: { $eq: ["$_id.tipo_especialidad", "tiempo"] },
          then: { $max: "$marcas" },
          else: { $min: "$marcas" }
        }
      }
    }
  },
  { $sort: { deportista: 1, especialidad: 1 } }
  ,   {
    $unset: "_id"
  }
]);

