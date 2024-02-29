db.lineitem.aggregate([
    {
      $match: {
        L_SHIPDATE: {
          $gte: ISODate("1994-01-01"),
          $lt: ISODate("1995-01-01")
        },
        L_DISCOUNT: {
          $gte: 0.02,
          $lte: 0.04
        },
        L_QUANTITY: { $lt: 25 }
      }
    },
    {
      $group: {
        _id: null,
        revenue: {
          $sum: { $multiply: ["$L_EXTENDEDPRICE", "$L_DISCOUNT"] }
        }
      }
    },
    { $limit: 1 }
  ]);
  

  db.lineitem.aggregate([
    {
      $match: {
        L_SHIPDATE: {
          $gte: ISODate("1993-01-01"),
          $lt: ISODate("1994-01-01")
        },
        L_DISCOUNT: {
          $gte: 0.07,
          $lte: 0.09
        },
        L_QUANTITY: { $lt: 24 }
      }
    },
    {
      $group: {
        _id: null,
        revenue: {
          $sum: { $multiply: ["$L_EXTENDEDPRICE", "$L_DISCOUNT"] }
        }
      }
    },
    { $limit: 1 }
  ]);
  