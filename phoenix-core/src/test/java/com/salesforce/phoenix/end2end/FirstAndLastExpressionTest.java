/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.salesforce.phoenix.end2end;

import com.salesforce.hbase.index.IndexTestingUtils;
import com.salesforce.hbase.index.Indexer;
import static com.salesforce.phoenix.end2end.BaseConnectedQueryTest.getUrl;
import static org.junit.Assert.*;
import java.sql.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 *
 * @author tzolkincz
 */
public class FirstAndLastExpressionTest extends BaseHBaseManagedTimeTest {

	protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

	private void prepareTable() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id INTEGER, date INTEGER, \"value\" INTEGER)";
		conn.createStatement().execute(ddl);


		//in \"value\"s will be 0, 2, 4, 6, 8, so 8 is the biggest
		for (int i = 0; i < 5; i++) {
			conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (" + i + ", 8, " + 100 * i + ", " + i * 2 + ")");
		}

		conn.commit();
	}

	@Test
	public void mrdka() throws Exception {


		Configuration conf = UTIL.getConfiguration();
		IndexTestingUtils.setupConfig(conf);
		// disable version checking, so we can test against whatever version of HBase happens to be
		// installed (right now, its generally going to be SNAPSHOT versions).
		conf.setBoolean(Indexer.CHECK_VERSION_CONF_KEY, false);
		UTIL.startMiniCluster();


		Connection conn = DriverManager.getConnection(getUrl());


		String ddl = "CREATE TABLE IF NOT EXISTS \"testFirstTable\" (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)";
		conn.createStatement().execute(ddl);
		ddl = "CREATE INDEX testFirstTableIndex on \"testFirstTable\" (val)";
		conn.createStatement().execute(ddl);

		conn.commit();
		conn.close();

		Thread.sleep(1000L);




		conf.setInt("hbase.client.operation.timeout", 10);
		conf.setInt("hbase.rpc.timeout", 10);
		conf.setInt("hbase.client.retries.number", 1);

		HBaseAdmin admin = UTIL.getHBaseAdmin();
		int a = 4;
		for (String t: admin.getTableNames()) {
			System.out.println(t);
			a ++;
		}
		if (true)
			return;


		HTable tableH = new HTable(conf, "\"testFirstTable\"");
		tableH.setAutoFlush(false);

		byte[] val = {(byte) 128, 0, 0, 12};
		Put put = new Put(val);
		put.add(Bytes.toBytes("_0"), Bytes.toBytes("VAL"), val);

		tableH.put(put);
		tableH.flushCommits();

		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM testFirstTableIndex");

		rs.next();
		System.out.println(rs.getInt(1));
		System.out.println(rs.getInt(2));

//		conn.createStatement().execute("UPSERT INTO testFirstTable (id, val) VALUES (1, 2)");
//		conn.commit();
	}

//	@Test
//  public void testMultipleTimestampsInSinglePut() throws Exception {
//    HTable primary = createSetupTables(fam1);
//
//    // do a put to the primary table
//    Put p = new Put(row1);
//    long ts1 = 10;
//    long ts2 = 11;
//    p.add(FAM, indexed_qualifer, ts1, value1);
//    p.add(FAM, regular_qualifer, ts1, value2);
//    // our group indexes all columns in the this family, so any qualifier here is ok
//    p.add(FAM2, regular_qualifer, ts2, value3);
//    primary.put(p);
//    primary.flushCommits();
//
//    // read the index for the expected values
//    HTable index1 = new HTable(UTIL.getConfiguration(), getIndexTableName());
//
//    // build the expected kvs
//    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
//    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
//    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));
//
//    // check the first entry at ts1
//    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts1, pairs);
//    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts1, value1);
//
//    // check the second entry at ts2
//    pairs.clear();
//    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
//    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
//    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
//    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts2, value1);
//
//    // cleanup
//    closeAndCleanupTables(primary, index1);
//  }

	@Test
	public void zKrtka() throws Exception {
		HBaseTestingUtility UTIL = new HBaseTestingUtility();


		Connection conn =  DriverManager.getConnection(getUrl());


	}


	@Test
	public void testFirst() throws Exception {
		prepareTable();
		Connection conn = DriverManager.getConnection(getUrl());

		ResultSet rs = conn.createStatement().executeQuery("SELECT FIRST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 0);
		assertFalse(rs.next());

	}

	@Test
	public void offsetValue() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date INTEGER, \"value\" UNSIGNED_LONG)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 0, 300)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 1, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 2, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 3, 4)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (6, 8, 5, 150)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT FIRST_BY(\"value\", date, 2) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 7);
		assertFalse(rs.next());
	}

	@Test
	public void offsetValueLast() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date INTEGER, \"value\" UNSIGNED_LONG)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 0, 300)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 1, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 2, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 3, 4)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (6, 8, 5, 150)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date, 2) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 2);
		assertFalse(rs.next());
	}

	@Test
	public void offsetValueLastMismatchByColumn() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date INTEGER, \"value\" UNSIGNED_LONG)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 5, 8)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 1, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 4, 4)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 3, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (6, 8, 0, 1)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date, 2) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 4);
		assertFalse(rs.next());
	}

	@Test
	public void unsignedLong() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date DATE, \"value\" UNSIGNED_LONG)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, TO_DATE('2013-01-01 00:00:00'), 300)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, TO_DATE('2013-01-01 00:01:00'), 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, TO_DATE('2013-01-01 00:02:00'), 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, TO_DATE('2013-01-01 00:03:00'), 4)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, TO_DATE('2013-01-01 00:04:00'), 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (6, 8, TO_DATE('2013-01-01 00:05:00'), 150)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		byte[] jebka = rs.getBytes(1);
		System.out.println(jebka);
		assertEquals(rs.getLong(1), 150);

		assertFalse(rs.next());
	}

	@Test
	public void signedLongAsBigInt() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date BIGINT, \"value\" BIGINT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 5, 158)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 4, 5)");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT FIRST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getLong(1), 3);
		assertFalse(rs.next());
	}

	@Test
	public void signedInteger() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date INTEGER, \"value\" INTEGER)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 5, -255)"); //this
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 4, 4)");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), -255);
		assertFalse(rs.next());
	}

	@Test
	public void unsignedInteger() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 5, 4)");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 4);
		assertFalse(rs.next());
	}

	@Test
	public void testLast() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 5, 4)");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 4);
		assertFalse(rs.next());
	}

	@Test
	public void doubleDataType() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date DOUBLE, \"value\" DOUBLE)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 1, 300)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 5, 400)");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT FIRST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals("doubles", rs.getDouble(1), 300, 0.00001);
		assertFalse(rs.next());
	}

	@Test
	public void charDatatype() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date CHAR(3), \"value\" CHAR(3))";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, '1', '300')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, '2', '7')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, '3', '9')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, '4', '2')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, '5', '400')");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getString(1), "400");
		assertFalse(rs.next());
	}

	@Test
	public void varcharFixedLenghtDatatype() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date VARCHAR(3), \"value\" VARCHAR(3))";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, '1', '3')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, '2', '7')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, '3', '9')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, '4', '2')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, '5', '4')");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT FIRST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getString(1), "3");
		assertFalse(rs.next());
	}

	@Test
	public void varcharVariableLenghtDatatype() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date VARCHAR, \"value\" VARCHAR)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, '1', '3')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, '2', '7')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, '3', '9')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, '4', '2')");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, '5', '4')");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getString(1), "4");
		assertFalse(rs.next());
	}

	@Test
	public void floatDataType() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date FLOAT, \"value\" FLOAT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 1, 300)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 5, 400)");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT FIRST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getFloat(1), 300.0, 0.000001);
		assertFalse(rs.next());

	}

	@Test
	public void groupMultipleValues() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		//first page_id
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 5, 4)");

		//second page_id
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (11, 9, 1, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (12, 9, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (13, 9, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (15, 9, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (14, 9, 5, 40)");

		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 4);

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 40);
		assertFalse(rs.next());
	}

	@Test
	public void nullValuesInAggregatingColumns() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		//first page_id
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (1, 8, 1)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (2, 8, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (3, 8, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (5, 8, 4)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (4, 8, 5)");
		conn.commit();


		//ResultSet rs = conn.createStatement().executeQuery("SELECT MIN(\"value\") FROM testFirstTable GROUP BY page_id");
		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");


		assertTrue(rs.next());
		//assertEquals(rs.getInt(1), 4);
		byte[] nothing = rs.getBytes(1);
		if (nothing == null) {
			assertTrue(true);
		} else {
			assertTrue(false);
		}
	}

	@Test
	public void nullValuesInAggregatingColumnsSecond() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		//first page_id
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (1, 8, 1)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (2, 8, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (3, 8, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (5, 8, 4)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date) VALUES (4, 8, 5)");
		conn.commit();



		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");
	}

	@Test
	public void allColumnsNull() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date FLOAT, \"value\" FLOAT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id) VALUES (1, 8)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id) VALUES (2, 8)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id) VALUES (3, 8)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id) VALUES (5, 8)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id) VALUES (4, 8)");
		conn.commit();


		ResultSet rs = conn.createStatement().executeQuery("SELECT FIRST_BY(\"value\", date) FROM testFirstTable GROUP BY page_id");

		assertTrue(rs.next());
		//assertEquals(rs.getInt(1), 4);
		byte[] nothing = rs.getBytes(1);
		if (nothing == null) {
			assertTrue(true);
		} else {
			assertTrue(false);
		}

	}


	@Test
	public void inOrderByClausule() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS testFirstTable (id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_INT, date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		//first page
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (4, 8, 5, 5)");

		//second page
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (5, 2, 1, 3)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (6, 2, 2, 7)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (7, 2, 3, 9)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (8, 2, 4, 2)");
		conn.createStatement().execute("UPSERT INTO testFirstTable (id, page_id, date, \"value\") VALUES (9, 2, 5, 4)");

		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT LAST_BY(\"value\", date), page_id FROM testFirstTable GROUP BY page_id ORDER BY LAST_BY(\"value\", date) DESC");


		assertTrue(rs.next());
		System.out.println(rs.getInt(1));
		System.out.println(rs.getInt(2));
		assertEquals(rs.getInt(1), 5);

		assertTrue(rs.next());
		System.out.println(rs.getInt(1));
		System.out.println(rs.getInt(2));
		assertEquals(rs.getInt(1), 4);
		assertFalse(rs.next());
	}
}