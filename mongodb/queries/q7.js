db.lineitem.aggregate([
    {
      $match: {
        L_SHIPDATE: {
          $gte: ISODate("1995-01-01"),
          $lte: ISODate("1996-12-31")
        }
      }
    },
    {
      $lookup: {
        from: "supplier",
        localField: "L_SUPPKEY",
        foreignField: "_id",
        as: "supplier"
      }
    },
    { $unwind: "$supplier" },
    {
      $lookup: {
        from: "orders",
        localField: "L_ORDERKEY",
        foreignField: "_id",
        as: "order"
      }
    },
    { $unwind: "$order" },
    {
      $lookup: {
        from: "customer",
        localField: "order.O_CUSTKEY",
        foreignField: "_id",
        as: "customer"
      }
    },
    { $unwind: "$customer" },
    {
      $lookup: {
        from: "nation",
        localField: "supplier.S_NATIONKEY",
        foreignField: "_id",
        as: "supplier_nation"
      }
    },
    { $unwind: "$supplier_nation" },
    {
      $lookup: {
        from: "nation",
        localField: "customer.C_NATIONKEY",
        foreignField: "_id",
        as: "customer_nation"
      }
    },
    { $unwind: "$customer_nation" },
    {
      $match: {
        $or: [
          { $and: [{ "supplier_nation.N_NAME": "CANADA" }, { "customer_nation.N_NAME": "ROMANIA" }] },
          { $and: [{ "supplier_nation.N_NAME": "ROMANIA" }, { "customer_nation.N_NAME": "CANADA" }] }
        ]
      }
    },
    {
      $group: {
        _id: {
          supp_nation: "$supplier_nation.N_NAME",
          cust_nation: "$customer_nation.N_NAME",
          l_year: { $year: "$L_SHIPDATE" }
        },
        revenue: { $sum: { $multiply: ["$L_EXTENDEDPRICE", { $subtract: [1, "$L_DISCOUNT"] }] } }
      }
    },
    {
      $sort: {
        "_id.supp_nation": 1,
        "_id.cust_nation": 1,
        "_id.l_year": 1
      }
    },
    { $limit: 1 }
  ]);

  db.lineitem.aggregate([
    {
      $match: {
        L_SHIPDATE: {
          $gte: ISODate("1995-01-01"),
          $lte: ISODate("1996-12-31")
        }
      }
    },
    {
      $lookup: {
        from: "supplier",
        localField: "L_SUPPKEY",
        foreignField: "_id",
        as: "supplier"
      }
    },
    { $unwind: "$supplier" },
    {
      $lookup: {
        from: "orders",
        localField: "L_ORDERKEY",
        foreignField: "_id",
        as: "order"
      }
    },
    { $unwind: "$order" },
    {
      $lookup: {
        from: "customer",
        localField: "order.O_CUSTKEY",
        foreignField: "_id",
        as: "customer"
      }
    },
    { $unwind: "$customer" },
    {
      $lookup: {
        from: "nation",
        localField: "supplier.S_NATIONKEY",
        foreignField: "_id",
        as: "supplier_nation"
      }
    },
    { $unwind: "$supplier_nation" },
    {
      $lookup: {
        from: "nation",
        localField: "customer.C_NATIONKEY",
        foreignField: "_id",
        as: "customer_nation"
      }
    },
    { $unwind: "$customer_nation" },
    {
      $match: {
        $or: [
          { $and: [{ "supplier_nation.N_NAME": "CHINA" }, { "customer_nation.N_NAME": "ARGENTINA" }] },
          { $and: [{ "supplier_nation.N_NAME": "ARGENTINA" }, { "customer_nation.N_NAME": "CHINA" }] }
        ]
      }
    },
    {
      $group: {
        _id: {
          supp_nation: "$supplier_nation.N_NAME",
          cust_nation: "$customer_nation.N_NAME",
          l_year: { $year: "$L_SHIPDATE" }
        },
        revenue: { $sum: { $multiply: ["$L_EXTENDEDPRICE", { $subtract: [1, "$L_DISCOUNT"] }] } }
      }
    },
    {
      $sort: {
        "_id.supp_nation": 1,
        "_id.cust_nation": 1,
        "_id.l_year": 1
      }
    },
    { $limit: 1 }
  ]);
  
  