package com.salesforce.phoenix.expression.function;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.FirstAndLastServerAggregator;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.parse.FirstAggregateParseNode;
import com.salesforce.phoenix.schema.ColumnModifier;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

@FunctionParseNode.BuiltInFunction(name = FirstFunction.NAME, nodeClass = FirstAggregateParseNode.class, args = {
	@FunctionParseNode.Argument(),
	@FunctionParseNode.Argument()})
public class FirstFunction extends FirstLastBaseFunction {

	public static final String NAME = "FIRST_BY";

	public FirstFunction() {
	}

	public FirstFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
		super(childExpressions, delegate);
	}

	@Override
	public Aggregator newServerAggregator(Configuration conf) {
		ColumnModifier columnModifier = getAggregatorExpression().getColumnModifier();

		FirstAndLastServerAggregator aggregator = new FirstAndLastServerAggregator(columnModifier);
		aggregator.init(children, true);

		return aggregator;
	}
}
