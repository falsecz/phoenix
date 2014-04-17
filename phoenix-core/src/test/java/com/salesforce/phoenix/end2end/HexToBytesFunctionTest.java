package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.end2end.BaseConnectedQueryTest.getUrl;
import com.salesforce.phoenix.schema.PDataType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author tzolkincz
 */
public class HexToBytesFunctionTest extends BaseHBaseManagedTimeTest {

	@Test
	public void shouldPass() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE mrdka ( page_offset_id BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (page_offset_id))";

		conn.createStatement().execute(ddl);
		PreparedStatement ps = conn.prepareStatement("UPSERT INTO mrdka (page_offset_id) VALUES (?)");

		byte[] kk = Bytes.add(PDataType.UNSIGNED_LONG.toBytes(2232594215l), PDataType.INTEGER.toBytes(-8));
		ps.setBytes(1, kk);
		//ps.setInt(2, -8);

		ps.execute();
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM mrdka WHERE page_offset_id = HEX_TO_BYTES('000000008512af277ffffff8')");

		assertTrue(rs.next());
	}



	@Test
	public void shouldFail() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE mrdka ( page_offset_id BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (page_offset_id))";

		conn.createStatement().execute(ddl);
		PreparedStatement ps = conn.prepareStatement("UPSERT INTO mrdka (page_offset_id) VALUES (?)");

		byte[] kk = Bytes.add(PDataType.UNSIGNED_LONG.toBytes(2232594215l), PDataType.INTEGER.toBytes(-9));
		ps.setBytes(1, kk);

		ps.execute();
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM mrdka WHERE page_offset_id = HEX_TO_BYTES('000000008512af277ffffff8')");

		assertFalse(rs.next());
	}
}