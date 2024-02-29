/* LINEITEM */
db.lineitem.find().forEach(function(doc) {
    var shipDate = new Date(doc.L_SHIPDATE);
    var commitDate = new Date(doc.L_COMMITDATE);
    var receiptDate = new Date(doc.L_RECEIPTDATE);

    db.lineitem.updateOne(
        { _id: doc._id },
        { $set: { 
            L_SHIPDATE: shipDate,
            L_COMMITDATE: commitDate,
            L_RECEIPTDATE: receiptDate
        }}
    );
});


/* ORDERS */ 
db.orders.find().forEach(function(doc) {
    var orderDate = new Date(doc.O_ORDERDATE);

    db.orders.updateOne(
        { _id: doc._id },
        { $set: { 
            O_ORDERDATE: orderDate
        }}
    );
});
