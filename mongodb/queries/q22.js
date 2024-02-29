db.customer.aggregate([
    {
      $match: {
        C_PHONE: { $regex: /^(19|24|11|25|30|20|15)/ },
        C_ACCTBAL: { $gt: 0.00 }
      }
    },
    {
      $group: {
        _id: { $substr: ["$C_PHONE", 0, 2] },
        avg_acctbal: { $avg: "$C_ACCTBAL" },
        customers: { $push: "$$ROOT" }
      }
    },
    {
      $match: {
        "customers": { $not: { $elemMatch: { "O_CUSTKEY": { $exists: true } } } }
      }
    },
    {
      $match: {
        "_id": { $in: ["19", "24", "11", "25", "30", "20", "15"] },
        "avg_acctbal": { $gt: 0.00 }
      }
    },
    {
      $project: {
        cntrycode: "$_id",
        c_acctbal: "$customers.C_ACCTBAL"
      }
    },
    {
      $unwind: "$c_acctbal"
    },
    {
      $group: {
        _id: "$cntrycode",
        numcust: { $sum: 1 },
        totacctbal: { $sum: "$c_acctbal" }
      }
    },
    {
      $sort: { _id: 1 }
    },
    {
      $limit: 1
    }
  ]);
  