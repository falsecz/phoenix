package com.salesforce.phoenix.expression.function;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.FirstAndLastBaseClientAggregator;
import com.salesforce.phoenix.expression.aggregator.FirstAndLastServerAggregator;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.parse.LastAggregateParseNode;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

@FunctionParseNode.BuiltInFunction(name = LastFunction.NAME, nodeClass = LastAggregateParseNode.class, args = {
	@FunctionParseNode.Argument(),
	@FunctionParseNode.Argument(),
    @FunctionParseNode.Argument(allowedTypes={PDataType.INTEGER}, isConstant=true, defaultValue="0")})
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
		aggregator.init(children, false, ((Number) ((LiteralExpression) children.get(2)).getValue()).intValue());

		return aggregator;
	}

	@Override
	public Aggregator newClientAggregator() {
		ColumnModifier columnModifier = getAggregatorExpression().getColumnModifier();

		FirstAndLastBaseClientAggregator aggregator = new FirstAndLastBaseClientAggregator(columnModifier);
		if (children.size() < 3) {
			//@TODO WTF?
			aggregator.init(children, 0);
		} else {
			aggregator.init(children, ((Number) ((LiteralExpression) children.get(2)).getValue()).intValue());
		}

		return aggregator;
	}
}
