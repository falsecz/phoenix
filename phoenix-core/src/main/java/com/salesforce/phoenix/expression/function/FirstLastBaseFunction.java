package com.salesforce.phoenix.expression.function;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.tuple.Tuple;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 *
 * @author tzolkincz
 */
abstract public class FirstLastBaseFunction extends DelegateConstantToCountAggregateFunction {

	public static String NAME = null;

	public FirstLastBaseFunction() {
	}

	public FirstLastBaseFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
		super(childExpressions, delegate);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		boolean wasEvaluated = super.evaluate(tuple, ptr);
		if (!wasEvaluated) {
			return false;
		}
		if (isConstantExpression()) {
			getAggregatorExpression().evaluate(tuple, ptr);
		}
		return true;
	}

	@Override
	public ColumnModifier getColumnModifier() {
		return getAggregatorExpression().getColumnModifier();
	}

	@Override
	public String getName() {
		return NAME;
	}
}
