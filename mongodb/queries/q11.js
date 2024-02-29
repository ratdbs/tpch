/*
-- using 1700116778 as a seed to the RNG


select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	PARTSUPP,
	SUPPLIER,
	NATION
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'UNITED KINGDOM'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				PARTSUPP,
				SUPPLIER,
				NATION
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'UNITED KINGDOM'
		)
order by
	value desc
LIMIT 1;-- using 1706940296 as a seed to the RNG


select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	PARTSUPP,
	SUPPLIER,
	NATION
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'RUSSIA'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				PARTSUPP,
				SUPPLIER,
				NATION
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'RUSSIA'
		)
order by
	value desc
LIMIT 1;
*/

db.partsupp.aggregate([
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
      $match: {
        "nation.N_NAME": "UNITED KINGDOM"
      }
    },
    {
      $group: {
        _id: "$PS_PARTKEY",
        value: {
          $sum: { $multiply: ["$PS_SUPPLYCOST", "$PS_AVAILQTY"] }
        }
      }
    },
    {
      $match: {
        value: { $gt: { $multiply: ["$value", 0.0001000000] } }
      }
    },
    {
      $sort: { value: -1 }
    },
    { $limit: 1 }
  ]);
  

  db.partsupp.aggregate([
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
      $match: {
        "nation.N_NAME": "RUSSIA"
      }
    },
    {
      $group: {
        _id: "$PS_PARTKEY",
        value: {
          $sum: { $multiply: ["$PS_SUPPLYCOST", "$PS_AVAILQTY"] }
        }
      }
    },
    {
      $match: {
        value: { $gt: { $multiply: ["$value", 0.0001000000] } }
      }
    },
    {
      $sort: { value: -1 }
    },
    { $limit: 1 }
  ]);
  