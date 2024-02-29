/* Q1: Pricing summary record query */

db.lineitem.aggregate([
  {
    $match: {
      L_SHIPDATE: { $lte: ISODate("1998-08-29") },
    },
  },
  {
    $group: {
      _id: {
        L_RETURNFLAG: '$L_RETURNFLAG',
        L_LINESTATUS: '$L_LINESTATUS',
      },
      sum_qty: { $sum: '$L_QUANTITY' },
      sum_base_price: { $sum: '$L_EXTENDEDPRICE' },
      sum_disc_price: {
        $sum: {
          $multiply: ['$L_EXTENDEDPRICE', { $subtract: [1, '$L_DISCOUNT'] }],
        },
      },
      sum_charge: {
        $sum: {
          $multiply: [
            '$L_EXTENDEDPRICE',
            { $subtract: [1, '$L_DISCOUNT'] },
            { $add: [1, '$L_TAX'] },
          ],
        },
      },
      avg_qty: {$avg: "$L_QUANTITY"},
      avg_price: {$avg: "$L_EXTENDEDPRICE"},
      avg_disc: { $avg: '$L_DISCOUNT' },
      count_order: {$sum: 1}
    },
  },
  {
    $project: {
      _id: 0,
      l_returnflag: "$_id.L_RETURNFLAG",
      l_linestatus: "$_id.L_LINESTATUS",
      sum_qty: 1,
      sum_base_price: 1,
      sum_disc_price: 1,
      sum_charge: 1,
      avg_qty: 1,
      avg_price: 1,
      avg_disc: 1,
      count_order: 1
    }
  },
  {
    $sort: {l_returnflag: 1, l_linestatus: 1 }
  },
  {
    $limit: 1
  }
])


db.lineitem.aggregate([
  {
    $match: {
      L_SHIPDATE: { $lte: ISODate("1998-10-01") },
    },
  },
  {
    $group: {
      _id: {
        L_RETURNFLAG: '$L_RETURNFLAG',
        L_LINESTATUS: '$L_LINESTATUS',
      },
      sum_qty: { $sum: '$L_QUANTITY' },
      sum_base_price: { $sum: '$L_EXTENDEDPRICE' },
      sum_disc_price: {
        $sum: {
          $multiply: ['$L_EXTENDEDPRICE', { $subtract: [1, '$L_DISCOUNT'] }],
        },
      },
      sum_charge: {
        $sum: {
          $multiply: [
            '$L_EXTENDEDPRICE',
            { $subtract: [1, '$L_DISCOUNT'] },
            { $add: [1, '$L_TAX'] },
          ],
        },
      },
      avg_qty: {$avg: "$L_QUANTITY"},
      avg_price: {$avg: "$L_EXTENDEDPRICE"},
      avg_disc: { $avg: '$L_DISCOUNT' },
      count_order: {$sum: 1}
    },
  },
  {
    $project: {
      _id: 0,
      l_returnflag: "$_id.L_RETURNFLAG",
      l_linestatus: "$_id.L_LINESTATUS",
      sum_qty: 1,
      sum_base_price: 1,
      sum_disc_price: 1,
      sum_charge: 1,
      avg_qty: 1,
      avg_price: 1,
      avg_disc: 1,
      count_order: 1
    }
  },
  {
    $sort: {l_returnflag: 1, l_linestatus: 1 }
  },
  {
    $limit: 1
  }
])
