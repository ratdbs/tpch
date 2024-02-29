db.customer.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "_id",
      foreignField: "O_CUSTKEY",
      as: "orders"
    }
  },
  {
    $unwind: "$orders"
  },
  {
    $match: {
      "orders.O_ORDERDATE": {
        $gte: ISODate("1994-01-01"),
        $lt: ISODate("1995-01-01")
      }
    }
  },
  {
    $lookup: {
      from: "lineitem",
      localField: "orders._id",
      foreignField: "L_ORDERKEY",
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
      as: "supplier"
    }
  },
  {
    $unwind: "$supplier"
  },
  {
    $lookup: {
      from: "supplier",
      localField: "C_NATIONKEY",
      foreignField: "S_NATIONKEY",
      as: "customer_supplier"
    }
  },
  {
    $unwind: "$customer_supplier"
  },
  {
    $lookup: {
      from: "nation",
      localField: "customer_supplier.S_NATIONKEY",
      foreignField: "_id",
      as: "nation"
    }
  },
  {
    $unwind: "$nation"
  },
  {
    $lookup: {
      from: "region",
      localField: "nation.N_REGIONKEY",
      foreignField: "_id",
      as: "region"
    }
  },
  {
    $unwind: "$region"
  },
  {
    $match: {
      "region.R_NAME": "AMERICA"
    }
  },
  {
    $group: {
      _id: "$nation.N_NAME",
      revenue: { $sum: { $multiply: ["$lineitems.L_EXTENDEDPRICE", { $subtract: [1, "$lineitems.L_DISCOUNT"] }] } }
    }
  },
  {
    $project: {
      _id: 0,
      n_name: "$_id",
      revenue: 1
    }
  },
  {
    $sort: { revenue: -1 }
  },
  {
    $limit: 1
  }
]);


db.customer.aggregate([
  {
    $lookup: {
      from: "orders",
      let: { customer_id: "$_id", c_nationkey: "$C_NATIONKEY" },
      pipeline: [
        { $match: { $expr: { $and: [ { $eq: ["$O_CUSTKEY", "$$customer_id"] }, { $gte: ["$O_ORDERDATE", ISODate("1994-01-01")] }, { $lt: ["$O_ORDERDATE", ISODate("1995-01-01")] } ] } } },
        { $lookup: {
            from: "lineitem",
            localField: "O_ORDERKEY",
            foreignField: "L_ORDERKEY",
            as: "lineitems"
          }
        },
        { $unwind: "$lineitems" },
        { $lookup: {
            from: "supplier",
            let: { lineitem_suppkey: "$lineitems.L_SUPPKEY" },
            pipeline: [
              { $match: { $expr: { $eq: ["$S_NATIONKEY", "$$C_NATIONKEY"] } } },
              { $lookup: {
                  from: "nation",
                  localField: "S_NATIONKEY",
                  foreignField: "_id",
                  as: "nation"
                }
              },
              { $unwind: "$nation" },
              { $lookup: {
                  from: "region",
                  localField: "nation.N_REGIONKEY",
                  foreignField: "_id",
                  pipeline: [{ $match: { R_NAME: "AMERICA" } }],
                  as: "region"
                }
              },
              { $match: { "region.R_NAME": "AMERICA" } }
            ],
            as: "supplier"
          }
        },
        { $unwind: "$supplier" }
      ],
      as: "orders"
    }
  },
  { $unwind: "$orders" },
  { $unwind: "$orders.lineitems" },
  {
    $group: {
      _id: { nation: "$orders.supplier.nation.N_NAME", year: { $year: "$orders.O_ORDERDATE" } },
      revenue: { $sum: { $multiply: ["$orders.lineitems.L_EXTENDEDPRICE", { $subtract: [1, "$orders.lineitems.L_DISCOUNT"] }] } }
    }
  },
  {
    $project: {
      _id: 0,
      nation: "$_id.nation",
      o_year: "$_id.year",
      revenue: 1
    }
  },
  { $sort: { nation: 1, o_year: -1 } },
  { $limit: 1 }
]);
