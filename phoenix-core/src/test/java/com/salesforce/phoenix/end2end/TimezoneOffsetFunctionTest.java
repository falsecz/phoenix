/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.end2end.BaseConnectedQueryTest.getUrl;
import com.salesforce.phoenix.schema.IllegalDataException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author tzolkincz
 */
public class TimezoneOffsetFunctionTest extends BaseClientMangedTimeTest {

	@Test
	public void testTimezoneOffset() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST"
				+ "(k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-02-02 00:00:00'))";
		conn.createStatement().execute(dml);
		dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (2, TO_DATE('2014-06-02 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, TIMEZONE_OFFSET('Indian/Cocos', dates) FROM TIMEZONE_OFFSET_TEST");

		assertTrue(rs.next());
		assertEquals(390, rs.getInt(3));
		assertTrue(rs.next());
		assertEquals(390, rs.getInt(3));

	}

	@Test
	public void testUnknownTimezone() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST"
				+ "(k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-02-02 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		try {
			ResultSet rs = conn.createStatement().executeQuery(
					"SELECT k1, dates, TIMEZONE_OFFSET('Unknown_Timezone', dates) FROM TIMEZONE_OFFSET_TEST");

			rs.next();
			assertEquals(0, rs.getInt(3));
			fail();
		} catch (IllegalDataException e) {
			assertTrue(true);
			return;
		}
		fail();

	}

	@Test
	public void testInRowKeyDSTTimezoneDesc() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST "
				+ "(k1 INTEGER NOT NULL, dates DATE NOT NULL CONSTRAINT pk PRIMARY KEY (k1, dates DESC))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-02-02 00:00:00'))";
		conn.createStatement().execute(dml);
		dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (2, TO_DATE('2014-06-02 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, TIMEZONE_OFFSET('Europe/Prague', dates)"
						+ "FROM TIMEZONE_OFFSET_TEST ORDER BY k1 ASC");

		assertTrue(rs.next());
		assertEquals(60, rs.getInt(3));
		assertTrue(rs.next());
		assertEquals(120, rs.getInt(3));
	}
}
