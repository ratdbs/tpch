let piepeline = [
    {
      $lookup: {
        from: "supplier",
        localField: "PS_SUPPKEY",
        foreignField: "_id",
        as: "supplier"
      }
    },
    { $unwind: "$supplier" },
    {
      $lookup: {
        from: "nation",
        localField: "supplier.S_NATIONKEY",
        foreignField: "_id",
        as: "nation"
      }
    },
    { $unwind: "$nation" },
    {
      $lookup: {
        from: "region",
        localField: "nation.N_REGIONKEY",
        foreignField: "_id",
        as: "region"
      }
    },
    { $unwind: "$region" },
    {
      $match: {
        "region.R_NAME": "AFRICA"
      }
    },
    {
      $group: {
        _id: "$PS_PARTKEY",
        minSupplyCost: { $min: "$PS_SUPPLYCOST" }
      }
    }  
]

db.partsupp.aggregate(pipeline).forEach(function(doc) {
  db.minSupplyCosts.insert(doc);
});

db.part.aggregate([
  {
    $match: {
      P_SIZE: 41,
      P_TYPE: { $regex: /BRASS$/ }
    }
  },
  {
    $lookup: {
      from: "partsupp",
      localField: "_id",
      foreignField: "PS_PARTKEY",
      as: "part_supp"
    }
  },
  { $unwind: "$part_supp" },
  // Join the minimum supply cost information
  {
    $lookup: {
      from: "minSupplyCosts", // This is the collection with the pre-aggregated min ps_supplycost
      localField: "_id",
      foreignField: "_id", // Assuming the _id field here matches the part key
      as: "min_supply_cost_info"
    }
  },
  { $unwind: "$min_supply_cost_info" },
  // Ensure we only consider partsupp entries that match the minimum supply cost
  {
    $match: {
      $expr: {
        $eq: ["$part_supp.PS_SUPPLYCOST", "$min_supply_cost_info.minSupplyCost"]
      }
    }
  },
  // The remaining steps are similar to your original pipeline,
  // adjusted for the correct filtering based on minimum supply cost
  {
    $lookup: {
      from: "supplier",
      localField: "part_supp.PS_SUPPKEY",
      foreignField: "_id",
      as: "supplier"
    }
  },
  { $unwind: "$supplier" },
  {
    $lookup: {
      from: "nation",
      localField: "supplier.S_NATIONKEY",
      foreignField: "_id",
      as: "nation"
    }
  },
  { $unwind: "$nation" },
  {
    $lookup: {
      from: "region",
      localField: "nation.N_REGIONKEY",
      foreignField: "_id",
      as: "region"
    }
  },
  { $unwind: "$region" },
  {
    $match: {
      "region.R_NAME": "AFRICA"
    }
  },
  {
    $group: {
      _id: "$_id",
      s_acctbal: { $first: "$supplier.S_ACCTBAL" },
      s_name: { $first: "$supplier.S_NAME" },
      n_name: { $first: "$nation.N_NAME" },
      p_mfgr: { $first: "$P_MFGR" },
      s_address: { $first: "$supplier.S_ADDRESS" },
      s_phone: { $first: "$supplier.S_PHONE" },
      s_comment: { $first: "$supplier.S_COMMENT" },
      ps_supplycost: { $first: "$part_supp.PS_SUPPLYCOST" }
    }
  },
  {
    $sort: {
      s_acctbal: -1,
      n_name: 1,
      s_name: 1,
      _id: 1
    }
  },
  {
      $project:{
          _id: 0,
          s_acctbal: 1,
          s_name: 1,
          n_name: 1,
          p_mfgr: 1,
          s_address: 1,
          s_phone: 1,
          s_comment: 1
      }
  },
  {
    $limit: 100
  }
]);


db.part.aggregate([
    {
        $match: {
          P_SIZE: 41,
          P_TYPE: { $regex: /BRASS/ }
        }
    },
    {
      $lookup: {
        from: "partsupp",
        localField: "_id",
        foreignField: "PS_PARTKEY",
        as: "part_supp"
      }
    },
    {
      $unwind: "$part_supp"
    },
    {
      $lookup: {
        from: "supplier",
        localField: "part_supp.PS_SUPPKEY",
        foreignField: "_id",
        as: "supplier"
      }
    },
    {
      $unwind: "$supplier"
    },
    {
      $lookup: {
        from: "nation",
        localField: "supplier.S_NATIONKEY",
        foreignField: "_id",
        as: "nation"
      }
    },
    {
      $unwind: "$nation"
    },
    {
      $lookup: {
        from: "region",
        localField: "nation.N_REGIONKEY",
        foreignField: "_id",
        as: "region"
      }
    },
    {
      $unwind: "$region"
    },
    {
      $match: {
        "region.R_NAME": "AFRICA"
      }
    },
    {
      $group: {
        _id: "$_id",
        s_acctbal: { $first: "$supplier.S_ACCTBAL" },
        s_name: { $first: "$supplier.S_NAME" },
        n_name: { $first: "$nation.N_NAME" },
        p_mfgr: { $first: "$P_MFGR" },
        s_address: { $first: "$supplier.S_ADDRESS" },
        s_phone: { $first: "$supplier.S_PHONE" },
        s_comment: { $first: "$supplier.S_COMMENT" },
        ps_supplycost: { $first: "$part_supp.PS_SUPPLYCOST" }
      }
    },
    {
      $sort: {
        s_acctbal: -1,
        n_name: 1,
        s_name: 1,
        _id: 1
      }
    },
    {
        $project:{
            _id: 0,
            s_acctbal: 1,
            s_name: 1,
            n_name: 1,
            p_mfgr: 1,
            s_address: 1,
            s_phone: 1,
            s_comment: 1
        }
    },
    {
      $limit: 100
    }
  ]);
  

  db.part.aggregate([
    {
      $match: {
        P_SIZE: 41,
        P_TYPE: { $regex: /BRASS/ }
      }
    },
    {
      $lookup: {
        from: "partsupp",
        localField: "_id",
        foreignField: "PS_PARTKEY",
        as: "part_supp"
      }
    },
    {
      $unwind: "$part_supp"
    },
    {
      $lookup: {
        from: "supplier",
        localField: "part_supp.PS_SUPPKEY",
        foreignField: "_id",
        as: "supplier"
      }
    },
    {
      $unwind: "$supplier"
    },
    {
      $lookup: {
        from: "nation",
        localField: "supplier.S_NATIONKEY",
        foreignField: "_id",
        as: "nation"
      }
    },
    {
      $unwind: "$nation"
    },
    {
      $lookup: {
        from: "region",
        localField: "nation.N_REGIONKEY",
        foreignField: "_id",
        as: "region"
      }
    },
    {
      $unwind: "$region"
    },
    {
      $match: {
        "region.R_NAME": "AFRICA",
        "part_supp.PS_SUPPLYCOST": {
          $eq: (
            db.partsupp.aggregate([
              {
                $match: {
                  PS_SUPPKEY: "$part_supp.PS_SUPPKEY",
                  PS_PARTKEY: "$part_supp.PS_PARTKEY",
                  PS_NATIONKEY: "$supplier.S_NATIONKEY",
                  PS_REGIONKEY: "$region._id",
                  "region.R_NAME": "AFRICA"
                }
              },
              {
                $group: {
                  _id: null,
                  min_supplycost: { $min: "$PS_SUPPLYCOST" }
                }
              },
              {
                $project: {
                  _id: 0,
                  min_supplycost: 1
                }
              }
            ])
          )
        }
      }
    },
    {
      $sort: {
        s_acctbal: -1,
        n_name: 1,
        s_name: 1,
        _id: 1
      }
    },
    {
      $limit: 100
    }
  ]);
  