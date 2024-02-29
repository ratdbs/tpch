
db.lineitem.aggregate([
  {
    $group: {
      _id: "$L_PARTKEY",
      avg_quantity: { $avg: "$L_QUANTITY" }
    }
  },
  {
    $project: {
      _id: 0,
      agg_partkey: "$_id",
      avg_quantity: { $multiply: [0.2, "$avg_quantity"] }
    }
  },
  {
    $lookup: {
      from: "part",
      localField: "agg_partkey",
      foreignField: "_id",
      as: "part"
    }
  },
  {
    $unwind: "$part"
  },
  {
    $match: {
      "part.P_BRAND": "Brand#34",
      "part.P_CONTAINER": "MED JAR"
    }
  },
  {
    $lookup: {
      from: "lineitem",
      localField: "agg_partkey",
      foreignField: "L_PARTKEY",
      as: "lineitems"
    }
  },
  {
    $unwind: "$lineitems"
  },
  {
    $addFields: {
      "lineitems.avg_quantity": "$avg_quantity"
    }
  },
  {
    $match: {
      $expr: {
        $lt: ["$lineitems.L_QUANTITY", "$lineitems.avg_quantity"]
      }
    }
  },
  {
    $group: {
      _id: null,
      avg_yearly: { $sum: "$lineitems.L_EXTENDEDPRICE" }
    }
  },
  {
    $project: {
      _id: 0,
      avg_yearly: { $divide: ["$avg_yearly", 7.0] }
    }
  },
  {
    $limit: 1
  }
]);


db.lineitem.aggregate([
  {
    $group: {
      _id: "$L_PARTKEY",
      avg_quantity: { $avg: "$L_QUANTITY" }
    }
  },
  {
    $project: {
      _id: 0,
      agg_partkey: "$_id",
      avg_quantity: { $multiply: [0.2, "$avg_quantity"] }
    }
  },
  {
    $lookup: {
      from: "part",
      localField: "agg_partkey",
      foreignField: "_id",
      as: "part"
    }
  },
  {
    $unwind: "$part"
  },
  {
    $match: {
      "part.P_BRAND": "Brand#22",
      "part.P_CONTAINER": "JUMBO JAR"
    }
  },
  {
    $lookup: {
      from: "lineitem",
      localField: "agg_partkey",
      foreignField: "L_PARTKEY",
      as: "lineitems"
    }
  },
  {
    $unwind: "$lineitems"
  },
  {
    $addFields: {
      "lineitems.avg_quantity": "$avg_quantity"
    }
  },
  {
    $match: {
      $expr: {
        $lt: ["$lineitems.L_QUANTITY", "$lineitems.avg_quantity"]
      }
    }
  },
  {
    $group: {
      _id: null,
      avg_yearly: { $sum: "$lineitems.L_EXTENDEDPRICE" }
    }
  },
  {
    $project: {
      _id: 0,
      avg_yearly: { $divide: ["$avg_yearly", 7.0] }
    }
  },
  {
    $limit: 1
  }
]);
