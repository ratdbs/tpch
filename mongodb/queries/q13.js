/*
-- using 1700116778 as a seed to the RNG


select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			CUSTOMER left outer join ORDERS on
				c_custkey = o_custkey
				and o_comment not like '%special%accounts%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc
LIMIT 1;-- using 1706940296 as a seed to the RNG


select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			CUSTOMER left outer join ORDERS on
				c_custkey = o_custkey
				and o_comment not like '%unusual%packages%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc
LIMIT 1;
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
    {
      $addFields: {
        c_count: {
          $size: {
            $filter: {
              input: "$orders",
              as: "order",
              cond: { $not: {
                $regexMatch: {
                  input: "$$order.o_comment", // orders의 o_comment 필드
                  regex: "special.*accounts", 
                  options: "i" // 옵션: 대소문자 구분하지 않음
                }
              }
            }
          }
        }
      }
      }
    },
    {
      $group: {
        _id: "$c_count",
        custdist: { $sum: 1 }
      }
    },
    {
      $sort: { custdist: -1, _id: -1 }
    },
    { $limit: 1 }
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
    {
      $addFields: {
        c_count: {
          $size: {
            $filter: {
              input: "$orders",
              as: "order",
              cond: { $not: {
                $regexMatch: {
                  input: "$$order.o_comment", // orders의 o_comment 필드
                  regex: "usual.*packages", 
                  options: "i" // 옵션: 대소문자 구분하지 않음
                }
              }
            }
          }
        }
      }
      }
    },
    {
      $group: {
        _id: "$c_count",
        custdist: { $sum: 1 }
      }
    },
    {
      $sort: { custdist: -1, _id: -1 }
    },
    { $limit: 1 }
  ]);