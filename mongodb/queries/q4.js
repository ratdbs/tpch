
db.orders.aggregate([
    {
      $match: {
        O_ORDERDATE: {
          $gte: ISODate("1993-06-01"),
          $lt: ISODate("1993-09-01")
        }
      }
    },
    {
      $lookup: {
        from: "lineitem",
        let: { orderKey: "$_id" },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ["$L_ORDERKEY", "$$orderKey"] },
                  { $lt: ["$L_COMMITDATE", "$L_RECEIPTDATE"] }
                ]
              }
            }
          }
        ],
        as: "lineitems"
      }
    },
    {
      $match: {
        lineitems: { $exists: true, $not: { $size: 0 } }
      }
    },
    {
      $group: {
        _id: "$O_ORDERPRIORITY",
        order_count: { $sum: 1 }
      }
    },
    {
      $sort: {
        "_id": 1
      }
    },
    {
      $limit: 1
    }
  ]);


  db.orders.aggregate([
    {
      $match: {
        O_ORDERDATE: {
          $gte: ISODate("1997-06-01"),
          $lt: ISODate("1997-09-01")
        }
      }
    },
    {
      $lookup: {
        from: "lineitem",
        let: { orderKey: "$_id" },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ["$L_ORDERKEY", "$$orderKey"] },
                  { $lt: ["$L_COMMITDATE", "$L_RECEIPTDATE"] }
                ]
              }
            }
          }
        ],
        as: "lineitems"
      }
    },
    {
      $match: {
        lineitems: { $exists: true, $not: { $size: 0 } }
      }
    },
    {
      $group: {
        _id: "$O_ORDERPRIORITY",
        order_count: { $sum: 1 }
      }
    },
    {
      $sort: {
        "_id": 1
      }
    },
    {
      $limit: 1
    }
  ]);
  