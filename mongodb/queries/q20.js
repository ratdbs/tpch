db.supplier.aggregate([
    {
      $lookup: {
        from: "nation",
        localField: "S_NATIONKEY",
        foreignField: "_id",
        as: "nation"
      }
    },
    { $unwind: "$nation" },
    {
      $match: {
        "nation.N_NAME": "INDONESIA"
      }
    },
    {
      $lookup: {
        from: "partsupp",
        let: { suppkey: "$_id" },
        pipeline: [
          {
            $lookup: {
              from: "lineitem",
              let: { partkey: "$PS_PARTKEY", suppkey: "$PS_SUPPKEY" },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        { $gte: ["$L_SHIPDATE", ISODate("1997-01-01")] },
                        { $lt: ["$L_SHIPDATE", ISODate("1998-01-01")] },
                        { $eq: ["$$partkey", "$L_PARTKEY"] },
                        { $eq: ["$$suppkey", "$L_SUPPKEY"] }
                      ]
                    }
                  }
                }
              ],
              as: "lineitems"
            }
          },
          {
            $group: {
              _id: { partkey: "$PS_PARTKEY", suppkey: "$PS_SUPPKEY" },
              agg_quantity: { $sum: "$lineitems.L_QUANTITY" }
            }
          }
        ],
        as: "partsupps"
      }
    },
    {
      $match: {
        "partsupps.agg_quantity": { $exists: true },
        "partsupps.agg_quantity": { $gt: 0.5 }
      }
    },
    { $limit: 1 },
    {
      $project: {
        s_name: 1,
        s_address: 1,
        _id: 0
      }
    },
    { $sort: { s_name: 1 } },
  ], {allowDiskUse: true});
  