/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.salesforce.phoenix.expression.function;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.schema.IllegalDataException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author tzolkincz
 */
@FunctionParseNode.BuiltInFunction(name = ConvertTimezoneFunction.NAME, args = {
	@FunctionParseNode.Argument(allowedTypes = {PDataType.DATE}),
	@FunctionParseNode.Argument(allowedTypes = {PDataType.VARCHAR}),
	@FunctionParseNode.Argument(allowedTypes = {PDataType.VARCHAR})})
public class ConvertTimezoneFunction extends ScalarFunction {

	public static final String NAME = "CONVERT_TZ";

	public ConvertTimezoneFunction() {
	}

	public ConvertTimezoneFunction(List<Expression> children) throws SQLException {
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
		Long date = (Long) PDataType.LONG.toObject(ptr.get(), ptr.getOffset(), ptr.getLength());

		if (!children.get(1).evaluate(tuple, ptr)) {
			return false;
		}
		String timezoneFrom = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());

		if (!children.get(2).evaluate(tuple, ptr)) {
			return false;
		}
		String timezoneTo = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());

		TimeZone tzFrom = TimeZone.getTimeZone(timezoneFrom);
		TimeZone tzTo = TimeZone.getTimeZone(timezoneTo);

		if (!timezoneFrom.equals(tzFrom.getID())) {
			throw new IllegalDataException("Illegal timezone: " + timezoneFrom);
		}
		if (!timezoneTo.equals(tzTo.getID())) {
			throw new IllegalDataException("Illegal timezone: " + timezoneTo);
		}

		long dateInUtc = date - tzFrom.getOffset(date);
		long dateInTo = dateInUtc + tzTo.getOffset(dateInUtc);

		ptr.set(PDataType.DATE.toBytes(new Date(dateInTo)));

		return true;
	}

	@Override
	public PDataType getDataType() {
		return PDataType.DATE;
	}
}
