/*
-- using 1700116778 as a seed to the RNG


select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			PART,
			SUPPLIER,
			LINEITEM,
			PARTSUPP,
			ORDERS,
			NATION
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%papaya%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc
LIMIT 1;-- using 1706940296 as a seed to the RNG


select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			PART,
			SUPPLIER,
			LINEITEM,
			PARTSUPP,
			ORDERS,
			NATION
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%lawn%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc
LIMIT 1;
*/

db.part.aggregate([
	{
	  $match: {
		P_NAME: /papaya/ // Using a regular expression to simulate the LIKE '%papaya%'
	  }
	},
	{
	  $lookup: {
		from: "partsupp",
		localField: "_id",
		foreignField: "PS_PARTKEY",
		as: "partsupp"
	  }
	},
	{ $unwind: "$partsupp" },
	{
	  $lookup: {
		from: "lineitem",
		let: { partkey: "$p_partkey", suppkey: "$partsupp.ps_suppkey" },
		pipeline: [
		  {
			$match: {
			  $expr: {
				$and: [
				  { $eq: ["$l_partkey", "$$partkey"] },
				  { $eq: ["$l_suppkey", "$$suppkey"] }
				]
			  }
			}
		  }
		],
		as: "lineitem"
	  }
	},
	{ $unwind: "$lineitem" },
	{
	  $lookup: {
		from: "ORDERS",
		localField: "lineitem.l_orderkey",
		foreignField: "o_orderkey",
		as: "orders"
	  }
	},
	{ $unwind: "$orders" },
	{
	  $lookup: {
		from: "SUPPLIER",
		localField: "lineitem.l_suppkey",
		foreignField: "s_suppkey",
		as: "supplier"
	  }
	},
	{ $unwind: "$supplier" },
	{
	  $lookup: {
		from: "NATION",
		localField: "supplier.s_nationkey",
		foreignField: "n_nationkey",
		as: "nation"
	  }
	},
	{ $unwind: "$nation" },
	{
	  $project: {
		nation: "$nation.n_name",
		o_year: { $year: "$orders.o_orderdate" },
		amount: {
		  $subtract: [
			{ $multiply: ["$lineitem.l_extendedprice", { $subtract: [1, "$lineitem.l_discount"] }] },
			{ $multiply: ["$partsupp.ps_supplycost", "$lineitem.l_quantity"] }
		  ]
		}
	  }
	},
	{
	  $group: {
		_id: { nation: "$nation", o_year: "$o_year" },
		sum_profit: { $sum: "$amount" }
	  }
	},
	{
	  $sort: { "_id.nation": 1, "_id.o_year": -1 }
	},
	{ $limit: 1 }
  ]);
  