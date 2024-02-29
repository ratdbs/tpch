db.partsupp.aggregate([
    {
      $lookup: {
        from: "part",
        localField: "PS_PARTKEY",
        foreignField: "_id",
        as: "part_info"
      }
    },
    {
      $match: {
        "part_info.P_BRAND": { $ne: "Brand#33" },
        "part_info.P_TYPE": { $not: /^ECONOMY POLISHED/ },
        "part_info.P_SIZE": { $in: [25, 39, 48, 21, 30, 5, 32, 40] }
      }
    },
    {
      $lookup: {
        from: "supplier",
        localField: "PS_SUPPKEY",
        foreignField: "_id",
        as: "supplier_info"
      }
    },
    {
      $match: {
        "supplier_info.S_COMMENT": { $not: /Customer Complaints/ }
      }
    },
    {
      $group: {
        _id: { p_brand: "$part_info.P_BRAND", p_type: "$part_info.P_TYPE", p_size: "$part_info.P_SIZE" },
        supplier_cnt: { $addToSet: "$PS_SUPPKEY" }
      }
    },
    {
      $project: {
        _id: 0,
        p_brand: "$_id.p_brand",
        p_type: "$_id.p_type",
        p_size: "$_id.p_size",
        supplier_cnt: { $size: "$supplier_cnt" }
      }
    },
    {
      $sort: {
        supplier_cnt: -1,
      }
    },
    { $limit: 1 }
  ]);


  db.partsupp.aggregate([
    {
      $lookup: {
        from: "part",
        localField: "PS_PARTKEY",
        foreignField: "_id",
        as: "part_info"
      }
    },
    {
      $match: {
        "part_info.P_BRAND": { $ne: "Brand#22" },
        "part_info.P_TYPE": { $not: /^MEDIUM BURNISHED/ },
        "part_info.P_SIZE": { $in: [35, 42, 47, 8, 38, 30, 32, 44] }
      }
    },
    {
      $lookup: {
        from: "supplier",
        localField: "PS_SUPPKEY",
        foreignField: "_id",
        as: "supplier_info"
      }
    },
    {
      $match: {
        "supplier_info.S_COMMENT": { $not: /Customer Complaints/ }
      }
    },
    {
      $group: {
        _id: { p_brand: "$part_info.P_BRAND", p_type: "$part_info.P_TYPE", p_size: "$part_info.P_SIZE" },
        supplier_cnt: { $addToSet: "$PS_SUPPKEY" }
      }
    },
    {
      $project: {
        _id: 0,
        p_brand: "$_id.p_brand",
        p_type: "$_id.p_type",
        p_size: "$_id.p_size",
        supplier_cnt: { $size: "$supplier_cnt" }
      }
    },
    {
      $sort: {
        supplier_cnt: -1,
      }
    },
    { $limit: 1 }
  ]);

