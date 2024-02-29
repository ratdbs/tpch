db.supplier.aggregate([
    {
      $lookup: {
        from: "lineitem",
        localField: "_id",
        foreignField: "L_SUPPKEY",
        as: "lineitems"
      }
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
      $lookup: {
        from: "nation",
        localField: "S_NATIONKEY",
        foreignField: "_id",
        as: "nation"
      }
    },
    {
      $match: {
        "orders.O_ORDERSTATUS": "F",
        "lineitems.L_RECEIPTDATE": { $gt: "$lineitems.L_COMMITDATE" },
        "nation.N_NAME": "UNITED KINGDOM",
        $expr: {
          $and: [
            {
              $ne: [ { $arrayElemAt: [ "$lineitems.L_SUPPKEY", 0 ] }, "$_id" ]
            },
            {
              $not: {
                $gt: [ { $arrayElemAt: [ "$lineitems.L_RECEIPTDATE", 0 ] }, { $arrayElemAt: [ "$lineitems.L_COMMITDATE", 0 ] } ]
              }
            }
          ]
        }
      }
    },
    {
      $group: {
        _id: "$S_NAME",
        numwait: { $sum: 1 }
      }
    },
    {
      $sort: {
        numwait: -1,
        _id: 1
      }
    },
    {
      $limit: 100
    }
  ]);
