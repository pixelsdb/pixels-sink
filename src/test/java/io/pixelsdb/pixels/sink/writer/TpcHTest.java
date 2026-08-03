/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pixelsdb.pixels.sink.writer;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

@Tag("integration")
class TpcHTest
{

    static Logger logger = LoggerFactory.getLogger(TpcHTest.class);
    static RetinaService retinaService;
    static MetadataService metadataService;
    static TransService transService;
    static String schemaName = "pixels_tpch";
    TransContext transContext;

    @BeforeAll
    public static void setUp()
    {
        retinaService = RetinaService.Instance();
        metadataService = MetadataService.Instance();
        transService = TransService.Instance();
    }

    @BeforeEach
    public void getTrans() throws TransException
    {
        transContext = transService.beginTrans(false);
    }

    @AfterEach
    public void commitTrans() throws TransException
    {
        transService.commitTrans(transContext.getTransId(), false);
    }


    @Test
    public void insertCustomer() throws RetinaException
    {
        /*
              Column    |     Type      | Extra | Comment
          --------------+---------------+-------+---------
           c_custkey    | bigint        |       |
           c_name       | varchar(25)   |       |
           c_address    | varchar(40)   |       |
           c_nationkey  | bigint        |       |
           c_phone      | char(15)      |       |
           c_acctbal    | decimal(15,2) |       |
           c_mktsegment | char(10)      |       |
           c_comment    | varchar(117)  |       |
          (8 rows)
         */
        String tableName = "customer";
        for (int i = 0; i < 10; ++i)
        {
            byte[][] cols = new byte[8][];

            cols[0] = Long.toString(i).getBytes(StandardCharsets.UTF_8);                          // c_custkey
            cols[1] = ("Customer_" + i).getBytes(StandardCharsets.UTF_8);                         // c_name
            cols[2] = ("Address_" + i).getBytes(StandardCharsets.UTF_8);                          // c_address
            cols[3] = Long.toString(i % 5).getBytes(StandardCharsets.UTF_8);                      // c_nationkey
            cols[4] = String.format("123-456-789%02d", i).getBytes(StandardCharsets.UTF_8);       // c_phone (char(15))
            cols[5] = String.format("%.2f", i * 1000.0).getBytes(StandardCharsets.UTF_8);         // c_acctbal (decimal)
            cols[6] = ("SEGMENT" + (i % 3)).getBytes(StandardCharsets.UTF_8);                     // c_mktsegment (char(10))
            cols[7] = ("This is customer " + i).getBytes(StandardCharsets.UTF_8);                 // c_comment

//            boolean result = retinaService.insertRecord(schemaName, tableName, cols, transContext.getTimestamp());
//            logger.info("Inserted customer #" + i + " → result: " + result);
//            Assertions.assertTrue(result);
        }
    }

    @Test
    public void insertRegion() throws RetinaException
    {
    /*
          Column    |     Type     | Extra | Comment
        ------------+--------------+-------+---------
         r_regionkey | bigint       |       |
         r_name      | char(25)     |       |
         r_comment   | varchar(152) |       |
        (3 rows)
     */
        String tableName = "region";
        int start = 5;
        int end = 10;
        for (int i = start; i < end; ++i)
        {
            byte[][] cols = new byte[3][];

            cols[0] = Long.toString(i).getBytes(StandardCharsets.UTF_8);                      // r_regionkey
            cols[1] = String.format("Region_%02d", i).getBytes(StandardCharsets.UTF_8);       // r_name (char(25))
            cols[2] = ("This is region number " + i).getBytes(StandardCharsets.UTF_8);        // r_comment

//            boolean result = retinaService.insertRecord(schemaName, tableName, cols, transContext.getTimestamp());
//            logger.info("Inserted region #" + i + " → result: " + result);
//            Assertions.assertTrue(result);
        }
    }

    @Test
    public void insertNation() throws RetinaException
    {
    /*
         Column     |     Type     | Extra | Comment
        ------------+--------------+-------+---------
         n_nationkey | bigint       |       |
         n_name      | char(25)     |       |
         n_regionkey | bigint       |       |
         n_comment   | varchar(152) |       |
    */
        String tableName = "nation";
        int start = 0;
        int end = 10;

        for (int i = start; i < end; ++i)
        {
            byte[][] cols = new byte[4][];

            cols[0] = Long.toString(i).getBytes(StandardCharsets.UTF_8);                      // n_nationkey
            cols[1] = String.format("Nation_%02d", i).getBytes(StandardCharsets.UTF_8);        // n_name (char(25))
            cols[2] = Long.toString(i % 5).getBytes(StandardCharsets.UTF_8);                  // n_regionkey
            cols[3] = ("This is nation number " + i).getBytes(StandardCharsets.UTF_8);        // n_comment

//            boolean result = retinaService.insertRecord(schemaName, tableName, cols, transContext.getTimestamp());
//            logger.info("Inserted nation #" + i + " → result: " + result);
//            Assertions.assertTrue(result);
        }
    }

    @Test
    public void insertLineItem() throws RetinaException
    {
        /*
         * Table: lineitem
         * Columns:
         *  l_orderkey      | bigint
         *  l_partkey       | bigint
         *  l_suppkey       | bigint
         *  l_linenumber    | integer
         *  l_quantity      | decimal(15,2)
         *  l_extendedprice | decimal(15,2)
         *  l_discount      | decimal(15,2)
         *  l_tax           | decimal(15,2)
         *  l_returnflag    | char(1)
         *  l_linestatus    | char(1)
         *  l_shipdate      | date
         *  l_commitdate    | date
         *  l_receiptdate   | date
         *  l_shipinstruct  | char(25)
         *  l_shipmode      | char(10)
         *  l_comment       | varchar(44)
         */
        String tableName = "lineitem";
        int recordCount = 10;

        for (int i = 0; i < recordCount; ++i)
        {
            byte[][] cols = new byte[16][];

            cols[0] = Long.toString(1000 + i).getBytes(StandardCharsets.UTF_8); // l_orderkey
            cols[1] = Long.toString(2000 + i).getBytes(StandardCharsets.UTF_8); // l_partkey
            cols[2] = Long.toString(3000 + i).getBytes(StandardCharsets.UTF_8); // l_suppkey
            cols[3] = Integer.toString(i + 1).getBytes(StandardCharsets.UTF_8); // l_linenumber
            cols[4] = String.format("%.2f", 10.0 + i).getBytes(StandardCharsets.UTF_8); // l_quantity
            cols[5] = String.format("%.2f", 100.0 + i * 10).getBytes(StandardCharsets.UTF_8); // l_extendedprice
            cols[6] = String.format("%.2f", 0.05).getBytes(StandardCharsets.UTF_8); // l_discount
            cols[7] = String.format("%.2f", 0.08).getBytes(StandardCharsets.UTF_8); // l_tax
            cols[8] = "R".getBytes(StandardCharsets.UTF_8); // l_returnflag
            cols[9] = "O".getBytes(StandardCharsets.UTF_8); // l_linestatus
            cols[10] = "2025-07-10".getBytes(StandardCharsets.UTF_8); // l_shipdate
            cols[11] = "2025-07-12".getBytes(StandardCharsets.UTF_8); // l_commitdate
            cols[12] = "2025-07-15".getBytes(StandardCharsets.UTF_8); // l_receiptdate
            cols[13] = String.format("DELIVER TO %d", i).getBytes(StandardCharsets.UTF_8); // l_shipinstruct
            cols[14] = "AIR".getBytes(StandardCharsets.UTF_8); // l_shipmode
            cols[15] = String.format("Order comment %d", i).getBytes(StandardCharsets.UTF_8); // l_comment

//            boolean result = retinaService.insertRecord(schemaName, tableName, cols, transContext.getTimestamp());
//            logger.info("Inserted lineitem #" + i + " → result: " + result);
//            Assertions.assertTrue(result);
        }
    }

    @Test
    public void insertOrders() throws RetinaException
    {
        /*
         * Table: orders
         * Columns:
         *  o_orderkey      | bigint
         *  o_custkey       | bigint
         *  o_orderstatus   | char(1)
         *  o_totalprice    | decimal(15,2)
         *  o_orderdate     | date
         *  o_orderpriority | char(15)
         *  o_clerk         | char(15)
         *  o_shippriority  | integer
         *  o_comment       | varchar(79)
         */
        String tableName = "orders";
        int recordCount = 10;

        for (int i = 0; i < recordCount; ++i)
        {
            byte[][] cols = new byte[9][];

            cols[0] = Long.toString(10000 + i).getBytes(StandardCharsets.UTF_8); // o_orderkey
            cols[1] = Long.toString(500 + i).getBytes(StandardCharsets.UTF_8); // o_custkey
            cols[2] = "O".getBytes(StandardCharsets.UTF_8); // o_orderstatus
            cols[3] = String.format("%.2f", 1234.56 + i * 10).getBytes(StandardCharsets.UTF_8); // o_totalprice
            cols[4] = String.format("2025-07-%02d", 10 + i).getBytes(StandardCharsets.UTF_8); // o_orderdate
            cols[5] = String.format("PRIORITY-%d", i % 5).getBytes(StandardCharsets.UTF_8); // o_orderpriority
            cols[6] = String.format("Clerk#%03d", i).getBytes(StandardCharsets.UTF_8); // o_clerk
            cols[7] = Integer.toString(i % 10).getBytes(StandardCharsets.UTF_8); // o_shippriority
            cols[8] = String.format("This is order %d", i).getBytes(StandardCharsets.UTF_8); // o_comment

//            boolean result = retinaService.insertRecord(schemaName, tableName, cols, transContext.getTimestamp());
//            logger.info("Inserted order #" + i + " → result: " + result);
//            Assertions.assertTrue(result);
        }
    }

    @Test
    public void insertPart() throws RetinaException
    {
        /*
         * Table: part
         * Columns:
         *  p_partkey     | bigint
         *  p_name        | varchar(55)
         *  p_mfgr        | char(25)
         *  p_brand       | char(10)
         *  p_type        | varchar(25)
         *  p_size        | integer
         *  p_container   | char(10)
         *  p_retailprice | decimal(15,2)
         *  p_comment     | varchar(23)
         */
        String tableName = "part";
        int recordCount = 10;

        for (int i = 0; i < recordCount; ++i)
        {
            byte[][] cols = new byte[9][];

            cols[0] = Long.toString(2000 + i).getBytes(StandardCharsets.UTF_8); // p_partkey
            cols[1] = ("PartName_" + i).getBytes(StandardCharsets.UTF_8); // p_name
            cols[2] = String.format("MFGR#%02d", i % 5).getBytes(StandardCharsets.UTF_8); // p_mfgr
            cols[3] = String.format("Brand#%d", i % 3).getBytes(StandardCharsets.UTF_8); // p_brand
            cols[4] = ("TYPE_" + (i % 4)).getBytes(StandardCharsets.UTF_8); // p_type
            cols[5] = Integer.toString(5 + i).getBytes(StandardCharsets.UTF_8); // p_size
            cols[6] = ("Box" + (i % 6)).getBytes(StandardCharsets.UTF_8); // p_container
            cols[7] = String.format("%.2f", 99.99 + i * 2.5).getBytes(StandardCharsets.UTF_8); // p_retailprice
            cols[8] = ("Comment_" + i).getBytes(StandardCharsets.UTF_8); // p_comment

//            boolean result = retinaService.insertRecord(schemaName, tableName, cols, transContext.getTimestamp());
//            logger.info("Inserted part #" + i + " → result: " + result);
//            Assertions.assertTrue(result);
        }
    }

    @Test
    public void insertPartSupp() throws RetinaException
    {
        /*
         * Table: partsupp
         * Columns:
         *  ps_partkey    | bigint
         *  ps_suppkey    | bigint
         *  ps_availqty   | integer
         *  ps_supplycost | decimal(15,2)
         *  ps_comment    | varchar(199)
         */
        String tableName = "partsupp";
        int recordCount = 10;

        for (int i = 0; i < recordCount; ++i)
        {
            byte[][] cols = new byte[5][];

            cols[0] = Long.toString(1000 + i).getBytes(StandardCharsets.UTF_8);        // ps_partkey
            cols[1] = Long.toString(500 + i).getBytes(StandardCharsets.UTF_8);         // ps_suppkey
            cols[2] = Integer.toString(300 + i * 10).getBytes(StandardCharsets.UTF_8); // ps_availqty
            cols[3] = String.format("%.2f", 50.0 + i * 5.5).getBytes(StandardCharsets.UTF_8); // ps_supplycost
            cols[4] = ("This is supplier comment #" + i).getBytes(StandardCharsets.UTF_8);    // ps_comment

//            boolean result = retinaService.insertRecord(schemaName, tableName, cols, transContext.getTimestamp());
//            logger.info("Inserted partsupp #" + i + " → result: " + result);
//            Assertions.assertTrue(result);
        }
    }

    @Test
    public void insertSupplier() throws RetinaException
    {
        /*
         * Table: supplier
         * Columns:
         *  s_suppkey   | bigint
         *  s_name      | char(25)
         *  s_address   | varchar(40)
         *  s_nationkey | bigint
         *  s_phone     | char(15)
         *  s_acctbal   | decimal(15,2)
         *  s_comment   | varchar(101)
         */
        String tableName = "supplier";
        int recordCount = 10;

        for (int i = 0; i < recordCount; ++i)
        {
            byte[][] cols = new byte[7][];

            cols[0] = Long.toString(2000 + i).getBytes(StandardCharsets.UTF_8);                  // s_suppkey
            cols[1] = String.format("Supplier_%02d", i).getBytes(StandardCharsets.UTF_8);        // s_name
            cols[2] = ("Address_" + i).getBytes(StandardCharsets.UTF_8);                         // s_address
            cols[3] = Long.toString(i % 5).getBytes(StandardCharsets.UTF_8);                     // s_nationkey
            cols[4] = String.format("987-654-32%03d", i).getBytes(StandardCharsets.UTF_8);       // s_phone
            cols[5] = String.format("%.2f", 5000.0 + i * 123.45).getBytes(StandardCharsets.UTF_8); // s_acctbal
            cols[6] = ("Supplier comment for ID " + i).getBytes(StandardCharsets.UTF_8);         // s_comment

//            boolean result = retinaService.insertRecord(schemaName, tableName, cols, transContext.getTimestamp());
//            logger.info("Inserted supplier #" + i + " → result: " + result);
//            Assertions.assertTrue(result);
        }
    }

}
