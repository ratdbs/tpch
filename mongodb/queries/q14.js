db.lineitem.aggregate([
    {
      $match: {
        L_SHIPDATE: {
          $gte: ISODate("1996-08-01"),
          $lt: ISODate("1996-09-01")
        }
      }
    },
    {
      $lookup: {
        from: "part",
        localField: "L_PARTKEY",
        foreignField: "_id",
        as: "part_info"
      }
    },
    {
      $addFields: {
        promo_revenue: {
          $sum: {
            $cond: {
              if: { $regexMatch: { input: { $arrayElemAt: ["$part_info.P_TYPE", 0] }, regex: /^PROMO/ } },
              then: { $multiply: [{ $subtract: [1, "$L_DISCOUNT"] }, "$L_EXTENDEDPRICE"] },
              else: 0
            }
          }
        },
        total_revenue: { $multiply: [{ $subtract: [1, "$L_DISCOUNT"] }, "$L_EXTENDEDPRICE"] }
      }
    },
    {
      $group: {
        _id: null,
        promo_revenue: { $sum: "$promo_revenue" },
        total_revenue: { $sum: "$total_revenue" }
      }
    },
    {
      $project: {
        _id: 0,
        promo_revenue: {
          $multiply: [
            { $divide: ["$promo_revenue", "$total_revenue"] },
            100.00
          ]
        }
      }
    },
    { $limit: 1 }
  ]);

  db.lineitem.aggregate([
    {
      $match: {
        L_SHIPDATE: {
          $gte: ISODate("1993-09-01"),
          $lt: ISODate("1993-10-01")
        }
      }
    },
    {
      $lookup: {
        from: "part",
        localField: "L_PARTKEY",
        foreignField: "_id",
        as: "part_info"
      }
    },
    {
      $addFields: {
        promo_revenue: {
          $sum: {
            $cond: {
              if: { $regexMatch: { input: { $arrayElemAt: ["$part_info.P_TYPE", 0] }, regex: /^PROMO/ } },
              then: { $multiply: [{ $subtract: [1, "$L_DISCOUNT"] }, "$L_EXTENDEDPRICE"] },
              else: 0
            }
          }
        },
        total_revenue: { $multiply: [{ $subtract: [1, "$L_DISCOUNT"] }, "$L_EXTENDEDPRICE"] }
      }
    },
    {
      $group: {
        _id: null,
        promo_revenue: { $sum: "$promo_revenue" },
        total_revenue: { $sum: "$total_revenue" }
      }
    },
    {
      $project: {
        _id: 0,
        promo_revenue: {
          $multiply: [
            { $divide: ["$promo_revenue", "$total_revenue"] },
            100.00
          ]
        }
      }
    },
    { $limit: 1 }
  ]);
  