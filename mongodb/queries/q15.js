
// create the view
db.createView("revenue0", "lineitem", [
  {
      $match: {
          L_SHIPDATE: {
              $gte: ISODate("1997-10-01"),
              $lt: ISODate("1998-01-01")
          }
      }
  },
  {
      $group: {
          _id: "$L_SUPPKEY",
          total_revenue: {
              $sum: {
                  $multiply: ["$L_EXTENDEDPRICE", { $subtract: [1, "$L_DISCOUNT"] }]
              }
          }
      }
  }
]);
print("CREATE VIEW")


// query to retrieve the data
var max_revenue = db.revenue0.aggregate([
  { $group: { _id: null, max_revenue: { $max: "$total_revenue" } } }
]).toArray()[0].max_revenue;

db.supplier.aggregate([
  {
      $lookup: {
          from: "revenue0",
          localField: "_id",
          foreignField: "_id",
          as: "revenue_info"
      }
  },
  { $unwind: "$revenue_info" },
  {
      $match: {
          "revenue_info.total_revenue": max_revenue
      }
  },
  {
      $project: {
          s_suppkey: "$_id",
          s_name: "$S_NAME",
          s_address: "$S_ADDRESS",
          s_phone: "$S_PHONE",
          total_revenue: "$revenue_info.total_revenue"
      }
  },
  { $sort: { _id: 1 } },
  { $limit: 1 }
]);

// drop the view after the first query
db.revenue0.drop();
print("DROP VIEW")


// create the view
db.createView("revenue0", "lineitem", [
  {
      $match: {
          L_SHIPDATE: {
              $gte: ISODate("1997-11-01"),
              $lt: ISODate("1998-02-01")
          }
      }
  },
  {
      $group: {
          _id: "$L_SUPPKEY",
          total_revenue: {
              $sum: {
                  $multiply: ["$L_EXTENDEDPRICE", { $subtract: [1, "$L_DISCOUNT"] }]
              }
          }
      }
  }
]);
print("CREATE VIEW")

// query to retrieve the data
var max_revenue = db.revenue0.aggregate([
  { $group: { _id: null, max_revenue: { $max: "$total_revenue" } } }
]).toArray()[0].max_revenue;

db.supplier.aggregate([
  {
      $lookup: {
          from: "revenue0",
          localField: "_id",
          foreignField: "_id",
          as: "revenue_info"
      }
  },
  { $unwind: "$revenue_info" },
  {
      $match: {
          "revenue_info.total_revenue": max_revenue
      }
  },
  {
      $project: {
          _id: 1,
          S_NAME: 1,
          S_ADDRESS: 1,
          S_PHONE: 1,
          total_revenue: "$revenue_info.total_revenue"
      }
  },
  { $sort: { _id: 1 } },
  { $limit: 1 }
]);

// drop the view after the second query
db.revenue0.drop();
print("DROP VIEW")
