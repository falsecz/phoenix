/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.salesforce.phoenix.expression.function;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.IllegalDataException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author tzolkincz
 */
@FunctionParseNode.BuiltInFunction(name = TimezoneOffsetFunction.NAME, args = {
	@FunctionParseNode.Argument(allowedTypes = {PDataType.VARCHAR}),
	@FunctionParseNode.Argument(allowedTypes = {PDataType.DATE})})
public class TimezoneOffsetFunction extends ScalarFunction {

	public static final String NAME = "TIMEZONE_OFFSET";
	private static final int MILIS_TO_MINUTES = 60 * 1000;
	private final Map<String, TimeZone> chachedTimeZones = new HashMap<String, TimeZone>();

	public TimezoneOffsetFunction() {
	}

	public TimezoneOffsetFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!children.get(0).evaluate(tuple, ptr)) {
			return false;
		}

		String timezone = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());

		if (!children.get(1).evaluate(tuple, ptr)) {
			return false;
		}

		if (!chachedTimeZones.containsKey(timezone)) {
			TimeZone tz = TimeZone.getTimeZone(timezone);
			if (!tz.getID().equals(timezone)) {
				throw new IllegalDataException("Invalid timezone " + timezone);
			}
			chachedTimeZones.put(timezone, tz);
		}

		ColumnModifier columnModifier = children.get(1).getColumnModifier();

		int offset = chachedTimeZones.get(timezone)
				.getOffset((Long) PDataType.LONG.toObject(ptr, columnModifier));

		ptr.set(PDataType.INTEGER.toBytes(offset / MILIS_TO_MINUTES));
		return true;
	}

	@Override
	public PDataType getDataType() {
		return PDataType.INTEGER;
	}

}
