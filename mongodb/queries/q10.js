/*
-- using 1700116778 as a seed to the RNG


select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	CUSTOMER,
	ORDERS,
	LINEITEM,
	NATION
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-10-01'
	and o_orderdate < date '1993-10-01' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
LIMIT 20;-- using 1706940296 as a seed to the RNG


select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	CUSTOMER,
	ORDERS,
	LINEITEM,
	NATION
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1994-06-01'
	and o_orderdate < date '1994-06-01' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
LIMIT 20;
*/

db.customer.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "_id",
      foreignField: "O_CUSTKEY",
      as: "orders"
    }
  },
  { $unwind: "$orders" },
  {
    $lookup: {
      from: "lineitem",
      localField: "orders._id",
      foreignField: "L_ORDERKEY",
      as: "lineitems"
    }
  },
  { $unwind: "$lineitems" },
  {
    $match: {
      "orders.O_ORDERDATE": {
        $gte: ISODate("1993-10-01"),
        $lt: ISODate("1994-01-01")
      },
      "lineitems.L_RETURNFLAG": "R"
    }
  },
  {
    $lookup: {
      from: "nation",
      localField: "C_NATIONKEY",
      foreignField: "_id",
      as: "nation"
    }
  },
  { $unwind: "$nation" },
  {
    $group: {
      _id: {
        c_custkey: "$_id",
        c_name: "$C_NAME",
        c_acctbal: "$C_ACCTBAL",
        c_phone: "$C_PHONE",
        n_name: "$nation.N_NAME",
        c_address: "$C_ADDRESS",
        c_comment: "$C_COMMENT"
      },
      revenue: {
        $sum: {
          $multiply: ["$lineitems.L_EXTENDEDPRICE", { $subtract: [1, "$lineitems.L_DISCOUNT"] }]
        }
      }
    }
  },
  { $sort: { revenue: -1 } },
  { $limit: 20 }
]);

db.customer.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "_id",
      foreignField: "O_CUSTKEY",
      as: "orders"
    }
  },
  { $unwind: "$orders" },
  {
    $lookup: {
      from: "lineitem",
      localField: "orders._id",
      foreignField: "L_ORDERKEY",
      as: "lineitems"
    }
  },
  { $unwind: "$lineitems" },
  {
    $match: {
      "orders.O_ORDERDATE": {
        $gte: ISODate("1994-06-01"),
        $lt: ISODate("1994-09-01")
      },
      "lineitems.L_RETURNFLAG": "R"
    }
  },
  {
    $lookup: {
      from: "nation",
      localField: "C_NATIONKEY",
      foreignField: "_id",
      as: "nation"
    }
  },
  { $unwind: "$nation" },
  {
    $group: {
      _id: {
        c_custkey: "$_id",
        c_name: "$C_NAME",
        c_acctbal: "$C_ACCTBAL",
        c_phone: "$C_PHONE",
        n_name: "$nation.N_NAME",
        c_address: "$C_ADDRESS",
        c_comment: "$C_COMMENT"
      },
      revenue: {
        $sum: {
          $multiply: ["$lineitems.L_EXTENDEDPRICE", { $subtract: [1, "$lineitems.L_DISCOUNT"] }]
        }
      }
    }
  },
  { $sort: { revenue: -1 } },
  { $limit: 20 }
]);
  