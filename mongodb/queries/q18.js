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
      $lookup: {
          from: "lineitem",
          localField: "orders._id",
          foreignField: "L_ORDERKEY",
          as: "lineitems"
      }
  },
  {
      $group: {
          _id: {
              c_name: "$C_NAME",
              c_custkey: "$C_CUSTKEY",
              o_orderkey: "$orders.O_ORDERKEY",
              o_orderdate: "$orders.O_ORDERDATE",
              o_totalprice: "$orders.O_TOTALPRICE"
          },
          total_quantity: { $sum: "$lineitems.L_QUANTITY" }
      }
  },
  {
      $match: {
          "lineitems": { $exists: true }
      }
  },
  {
      $group: {
          _id: {
              c_name: "$_id.C_NAME",
              c_custkey: "$_id._id",
              o_orderkey: "$_id._id",
              o_orderdate: "$_id.O_ORDERDATE",
              o_totalprice: "$_id.O_TOTALPRICE"
          },
          total_quantity: { $first: "$total_quantity" }
      }
  },
  {
      $match: {
          total_quantity: { $gt: 315 }
      }
  },
  {
      $sort: {
          "_id.o_totalprice": -1,
          "_id.o_orderdate": 1
      }
  },
  {
      $limit: 100
  }
], { allowDiskUse: true });

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
      $lookup: {
          from: "lineitem",
          localField: "orders._id",
          foreignField: "L_ORDERKEY",
          as: "lineitems"
      }
  },
  {
      $group: {
          _id: {
              c_name: "$C_NAME",
              c_custkey: "$C_CUSTKEY",
              o_orderkey: "$orders.O_ORDERKEY",
              o_orderdate: "$orders.O_ORDERDATE",
              o_totalprice: "$orders.O_TOTALPRICE"
          },
          total_quantity: { $sum: "$lineitems.L_QUANTITY" }
      }
  },
  {
      $match: {
          "lineitems": { $exists: true }
      }
  },
  {
      $group: {
          _id: {
              c_name: "$_id.C_NAME",
              c_custkey: "$_id._id",
              o_orderkey: "$_id._id",
              o_orderdate: "$_id.O_ORDERDATE",
              o_totalprice: "$_id.O_TOTALPRICE"
          },
          total_quantity: { $first: "$total_quantity" }
      }
  },
  {
      $match: {
          total_quantity: { $gt: 313 }
      }
  },
  {
      $sort: {
          "_id.o_totalprice": -1,
          "_id.o_orderdate": 1
      }
  },
  {
      $limit: 100
  }
], { allowDiskUse: true });
