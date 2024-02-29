db.customer.aggregate([
    {
        $match: {
            MKT_SEGMENT: 'FURNITURE'
        }
    },
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
            "orders.O_ORDERDATE": { $lt: ISODate("1995-03-03") },
            "lineitems.L_SHIPDATE": { $gt: ISODate("1995-03-03") }
        }
    },
    {
        $group: {
            _id: {
                l_orderkey: "$lineitems.L_ORDERKEY",
                o_orderdate: "$orders.O_ORDERDATE",
                o_shippriority: "$orders.O_SHIPPRIORITY"
            },
            revenue: { $sum: { $multiply: ["$lineitems.L_EXTENDEDPRICE", { $subtract: [1, "$lineitems.L_DISCOUNT"] }] } }
        }
    },
    {
        $sort: {
            "revenue": -1,
            "o_orderdate": 1
        }
    },
    { $limit: 10 },
    {
        $project: {
            _id: 0,
            l_orderkey: "$_id.l_orderkey",
            revenue: 1,
            o_orderdate: "$_id.o_orderdate",
            o_shippriority: "$_id.o_shippriority"
        }
    }
]);

db.customer.aggregate([
    {
        $match: {
            MKT_SEGMENT: 'MACHINERY'
        }
    },
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
            "orders.O_ORDERDATE": { $lt: ISODate("1995-03-29") },
            "lineitems.L_SHIPDATE": { $gt: ISODate("1995-03-29") }
        }
    },
    {
        $group: {
            _id: {
                l_orderkey: "$lineitems.L_ORDERKEY",
                o_orderdate: "$orders.O_ORDERDATE",
                o_shippriority: "$orders.O_SHIPPRIORITY"
            },
            revenue: { $sum: { $multiply: ["$lineitems.L_EXTENDEDPRICE", { $subtract: [1, "$lineitems.L_DISCOUNT"] }] } }
        }
    },
    {
        $sort: {
            "revenue": -1,
            "o_orderdate": 1
        }
    },
    { $limit: 10 },
    {
        $project: {
            _id: 0,
            l_orderkey: "$_id.l_orderkey",
            revenue: 1,
            o_orderdate: "$_id.o_orderdate",
            o_shippriority: "$_id.o_shippriority"
        }
    }
]);
