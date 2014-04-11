/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.end2end.BaseConnectedQueryTest.getUrl;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author tzolkincz
 */
public class ConvertTimezoneFunctionTest extends BaseHBaseManagedTimeTest {

	@Test
	public void testConvertTimezoneEurope() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, CONVERT_TZ(dates, 'UTC', 'Europe/Prague') FROM TIMEZONE_OFFSET_TEST");

		assertTrue(rs.next());
		assertEquals(1393635600000L, rs.getDate(3).getTime());

	}

	@Test
	public void testConvertTimezoneAmerica() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, CONVERT_TZ(dates, 'UTC', 'America/Adak') FROM TIMEZONE_OFFSET_TEST");

		assertTrue(rs.next());
		assertEquals(1393596000000L, rs.getDate(3).getTime());
	}
}
