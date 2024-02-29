db.lineitem.aggregate([
    {
      $lookup: {
        from: "part",
        localField: "L_PARTKEY",
        foreignField: "_id",
        as: "part"
      }
    },
    { $unwind: "$part" },
    {
      $match: {
        $or: [
          {
            "part.P_BRAND": "Brand#34",
            "part.P_CONTAINER": { $in: ["SM CASE", "SM BOX", "SM PACK", "SM PKG"] },
            "L_QUANTITY": { $gte: 6, $lte: 16 },
            "part.P_SIZE": { $gte: 1, $lte: 5 },
            "L_SHIPMODE": { $in: ["AIR", "AIR REG"] },
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
          },
          {
            "part.P_BRAND": "Brand#52",
            "part.P_CONTAINER": { $in: ["MED BAG", "MED BOX", "MED PKG", "MED PACK"] },
            "L_QUANTITY": { $gte: 18, $lte: 28 },
            "part.P_SIZE": { $gte: 1, $lte: 10 },
            "L_SHIPMODE": { $in: ["AIR", "AIR REG"] },
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
          },
          {
            "part.P_BRAND": "Brand#33",
            "part.P_CONTAINER": { $in: ["LG CASE", "LG BOX", "LG PACK", "LG PKG"] },
            "L_QUANTITY": { $gte: 22, $lte: 32 },
            "part.P_SIZE": { $gte: 1, $lte: 15 },
            "L_SHIPMODE": { $in: ["AIR", "AIR REG"] },
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
          }
        ]
      }
    },
    {
      $group: {
        _id: null,
        revenue: {
          $sum: { $multiply: ["$L_EXTENDEDPRICE", { $subtract: [1, "$L_DISCOUNT"] }] }
        }
      }
    },
    { $limit: 1 }
  ]);

  db.lineitem.aggregate([
    {
      $lookup: {
        from: "part",
        localField: "L_PARTKEY",
        foreignField: "_id",
        as: "part"
      }
    },
    { $unwind: "$part" },
    {
      $match: {
        $or: [
          {
            "part.P_BRAND": "Brand#45",
            "part.P_CONTAINER": { $in: ["SM CASE", "SM BOX", "SM PACK", "SM PKG"] },
            "L_QUANTITY": { $gte: 6, $lte: 16 },
            "part.P_SIZE": { $gte: 1, $lte: 5 },
            "L_SHIPMODE": { $in: ["AIR", "AIR REG"] },
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
          },
          {
            "part.P_BRAND": "Brand#51",
            "part.P_CONTAINER": { $in: ["MED BAG", "MED BOX", "MED PKG", "MED PACK"] },
            "L_QUANTITY": { $gte: 19, $lte: 29 },
            "part.P_SIZE": { $gte: 1, $lte: 10 },
            "L_SHIPMODE": { $in: ["AIR", "AIR REG"] },
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
          },
          {
            "part.P_BRAND": "Brand#43",
            "part.P_CONTAINER": { $in: ["LG CASE", "LG BOX", "LG PACK", "LG PKG"] },
            "L_QUANTITY": { $gte: 20, $lte: 30 },
            "part.P_SIZE": { $gte: 1, $lte: 15 },
            "L_SHIPMODE": { $in: ["AIR", "AIR REG"] },
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
          }
        ]
      }
    },
    {
      $group: {
        _id: null,
        revenue: {
          $sum: { $multiply: ["$L_EXTENDEDPRICE", { $subtract: [1, "$L_DISCOUNT"] }] }
        }
      }
    },
    { $limit: 1 }
  ]);
  