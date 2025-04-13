/********************** Tx1：Insert **********************/
START TRANSACTION;

INSERT INTO customer (
    c_custkey, c_name, c_address, c_nationkey,
    c_phone, c_acctbal, c_mktsegment, c_comment
) VALUES (
             1001,
             'Customer 1001',
             'Address 1001',
             1,
             '123-456-7890',
             1000.50,
             'AUTO',
             'Sample customer'
         );

INSERT INTO orders (
    o_orderkey, o_custkey, o_orderstatus, o_totalprice,
    o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
) VALUES (
             2001,
             1001,
             'O',
             500.75,
             CURRENT_DATE,
             '1-URGENT',
             'Clerk001',
             1,
             'First order'
         );

INSERT INTO lineitem (
    l_orderkey, l_partkey, l_suppkey, l_linenumber,
    l_quantity, l_extendedprice, l_discount, l_tax,
    l_returnflag, l_linestatus, l_shipdate, l_commitdate,
    l_receiptdate, l_shipinstruct, l_shipmode, l_comment
) VALUES
      (2001, 3001, 4001, 1, 5.0, 50.25, 0.05, 0.08, 'N', 'O', CURRENT_DATE, CURRENT_DATE, CURRENT_DATE, 'DELIVER IN PERSON', 'TRUCK', 'Line 1'),
      (2002, 3002, 4002, 1, 3.0, 299.99, 0.1, 0.07, 'R', 'F', CURRENT_DATE, CURRENT_DATE, CURRENT_DATE, 'COLLECT COD', 'AIR', 'Line 2');

INSERT INTO orders (
    o_orderkey, o_custkey, o_orderstatus, o_totalprice,
    o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
) VALUES (
             2002,
             1001,
             'F',
             899.99,
             CURRENT_DATE,
             '2-HIGH',
             'Clerk002',
             2,
             'Second order'
         );

COMMIT;

/********************** Tx2: Update **********************/
START TRANSACTION;

UPDATE orders
SET o_totalprice = 600.00,
    o_comment = 'Price updated'
WHERE o_orderkey = 2001;

UPDATE lineitem
SET l_discount = 0.08
WHERE l_orderkey = 2001;

COMMIT;

/********************** Tx3: Delete **********************/
START TRANSACTION;

DELETE FROM lineitem
WHERE l_orderkey = 2002;

DELETE FROM orders
WHERE o_orderkey = 2002;

COMMIT;