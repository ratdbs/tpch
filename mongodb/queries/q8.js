db.part.aggregate([
  {
    $lookup: {
      from: "lineitem",
      localField: "_id",
      foreignField: "L_PARTKEY",
      as: "lineitems"
    }
  },
  {
    $unwind: "$lineitems"
  },
  {
    $lookup: {
      from: "supplier",
      localField: "lineitems.L_SUPPKEY",
      foreignField: "_id",
      as: "suppliers"
    }
  },
  {
    $unwind: "$suppliers"
  },
  {
    $lookup: {
      from: "orders",
      localField: "lineitems.L_ORDERKEY",
      foreignField: "_id",
      as: "orders"
    }
  },
  {
    $unwind: "$orders"
  },
  {
    $lookup: {
      from: "customer",
      localField: "orders.O_CUSTKEY",
      foreignField: "_id",
      as: "customers"
    }
  },
  {
    $unwind: "$customers"
  },
  {
    $lookup: {
      from: "nation",
      localField: "customers.C_NATIONKEY",
      foreignField: "_id",
      as: "nation1"
    }
  },
  {
    $unwind: "$nation1"
  },
  {
    $lookup: {
      from: "region",
      localField: "nation1.N_REGIONKEY",
      foreignField: "_id",
      as: "region"
    }
  },
  {
    $unwind: "$region"
  },
  {
    $lookup: {
      from: "nation",
      localField: "suppliers.S_NATIONKEY",
      foreignField: "_id",
      as: "nation2"
    }
  },
  {
    $unwind: "$nation2"
  },
  {
    $match: {
      "region.R_NAME": "EUROPE",
      "orders.O_ORDERDATE": {
        $gte: ISODate("1995-01-01"),
        $lte: ISODate("1996-12-31")
      },
      "P_TYPE": "LARGE BRUSHED COPPER"
    }
  },
  {
    $group: {
      _id: {
        o_year: { $year: "$orders.O_ORDERDATE" },
        nation: "$nation2.N_NAME"
      },
      volume: { $sum: { $multiply: ["$lineitems.L_EXTENDEDPRICE", { $subtract: [1, "$lineitems.L_DISCOUNT"] }] } }
    }
  },
  {
    $group: {
      _id: "$_id.o_year",
      totalVolume: { $sum: "$volume" },
      totalVolumeForRomania: {
        $sum: {
          $cond: [{ $eq: ["$_id.nation", "ROMANIA"] }, "$volume", 0]
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      o_year: "$_id",
      mkt_share: { $divide: ["$totalVolumeForRomania", "$totalVolume"] }
    }
  },
  {
    $sort: { o_year: 1 }
  },
  {
    $limit: 1
  }
]);
