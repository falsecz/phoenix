package com.salesforce.phoenix.expression.function;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.FirstAndLastServerAggregator;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.parse.LastAggregateParseNode;
import com.salesforce.phoenix.schema.ColumnModifier;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

@FunctionParseNode.BuiltInFunction(name = LastFunction.NAME, nodeClass = LastAggregateParseNode.class, args = {
	@FunctionParseNode.Argument(),
	@FunctionParseNode.Argument()})
public class LastFunction extends FirstLastBaseFunction {

	public static final String NAME = "LAST_BY";

	public LastFunction() {
	}

	public LastFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
		super(childExpressions, delegate);
	}

	@Override
	public Aggregator newServerAggregator(Configuration conf) {
		ColumnModifier columnModifier = getAggregatorExpression().getColumnModifier();

		FirstAndLastServerAggregator aggregator = new FirstAndLastServerAggregator(columnModifier);
		aggregator.init(children, false);

		return aggregator;
	}
}
