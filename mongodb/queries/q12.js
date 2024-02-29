/*
-- using 1700116778 as a seed to the RNG

select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	ORDERS,
	LINEITEM
where
	o_orderkey = l_orderkey
	and l_shipmode in ('SHIP', 'AIR')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1996-01-01'
	and l_receiptdate < date '1996-01-01' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode
LIMIT 1;-- using 1706940296 as a seed to the RNG


select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	ORDERS,
	LINEITEM
where
	o_orderkey = l_orderkey
	and l_shipmode in ('SHIP', 'MAIL')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1993-01-01'
	and l_receiptdate < date '1993-01-01' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode
LIMIT 1;
*/
db.orders.aggregate([
    {
      $lookup: {
        from: "lineitem",
        localField: "_id",
        foreignField: "L_ORDERKEY",
        as: "lineitems"
      }
    },
    { $unwind: "$lineitems" },
    {
      $match: {
        "lineitems.L_SHIPMODE": { $in: ["SHIP", "AIR"] },
        $expr: {
          $and: [
            { $lt: ["$lineitems.L_COMMITDATE", "$lineitems.L_RECEIPTDATE"] },
            { $lt: ["$lineitems.L_SHIPDATE", "$lineitems.L_COMMITDATE"] }
          ]
        },
        "lineitems.L_RECEIPTDATE": {
          $gte: ISODate("1996-01-01"),
          $lt: ISODate("1997-01-01")
        }
      }
    },
    {
      $group: {
        _id: "$lineitems.L_SHIPMODE",
        high_line_count: {
          $sum: {
            $cond: [
              {
                $or: [
                  { $eq: ["$O_ORDERPRIORITY", "1-URGENT"] },
                  { $eq: ["$O_ORDERPRIORITY", "2-HIGH"] }
                ]
              },
              1,
              0
            ]
          }
        },
        low_line_count: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ["$O_ORDERPRIORITY", "1-URGENT"] },
                  { $ne: ["$O_ORDERPRIORITY", "2-HIGH"] }
                ]
              },
              1,
              0
            ]
          }
        }
      }
    },
    { $sort: { _id: 1 } },
    { $limit: 1 }
  ]);

  db.orders.aggregate([
    {
      $lookup: {
        from: "lineitem",
        localField: "_id",
        foreignField: "L_ORDERKEY",
        as: "lineitems"
      }
    },
    { $unwind: "$lineitems" },
    {
      $match: {
        "lineitems.L_SHIPMODE": { $in: ["SHIP", "MAIL"] },
        $expr: {
          $and: [
            { $lt: ["$lineitems.L_COMMITDATE", "$lineitems.L_RECEIPTDATE"] },
            { $lt: ["$lineitems.L_SHIPDATE", "$lineitems.L_COMMITDATE"] }
          ]
        },
        "lineitems.L_RECEIPTDATE": {
          $gte: ISODate("1993-01-01"),
          $lt: ISODate("1994-01-01")
        }
      }
    },
    {
      $group: {
        _id: "$lineitems.L_SHIPMODE",
        high_line_count: {
          $sum: {
            $cond: [
              {
                $or: [
                  { $eq: ["$O_ORDERPRIORITY", "1-URGENT"] },
                  { $eq: ["$O_ORDERPRIORITY", "2-HIGH"] }
                ]
              },
              1,
              0
            ]
          }
        },
        low_line_count: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ["$O_ORDERPRIORITY", "1-URGENT"] },
                  { $ne: ["$O_ORDERPRIORITY", "2-HIGH"] }
                ]
              },
              1,
              0
            ]
          }
        }
      }
    },
    { $sort: { _id: 1 } },
    { $limit: 1 }
  ]);

