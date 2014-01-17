/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.salesforce.phoenix.expression.function;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.FirstAndLastBaseClientAggregator;
import com.salesforce.phoenix.expression.aggregator.FirstAndLastServerAggregator;
import com.salesforce.phoenix.parse.FirstAggregateParseNode;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import java.util.List;
import org.apache.hadoop.conf.Configuration;


@FunctionParseNode.BuiltInFunction(name = Jebka.NAME, nodeClass = FirstAggregateParseNode.class, args = {
	@FunctionParseNode.Argument(),
	@FunctionParseNode.Argument(),
    @FunctionParseNode.Argument(allowedTypes={PDataType.INTEGER}, isConstant=true)})
public class Jebka extends FirstLastBaseFunction {

	public static final String NAME = "JEBKA";

	public Jebka() {
	}

	public Jebka(List<Expression> childExpressions, CountAggregateFunction delegate) {
		super(childExpressions, delegate);
	}


	@Override
	public Aggregator newClientAggregator() {
		ColumnModifier columnModifier = getAggregatorExpression().getColumnModifier();

		FirstAndLastBaseClientAggregator aggregator = new FirstAndLastBaseClientAggregator(columnModifier);

		aggregator.init(children, ((Number) ((LiteralExpression) children.get(2)).getValue()).intValue());

		return aggregator;
	}

	@Override
	public Aggregator newServerAggregator(Configuration conf) {
		ColumnModifier columnModifier = getAggregatorExpression().getColumnModifier();

		FirstAndLastServerAggregator aggregator = new FirstAndLastServerAggregator(columnModifier);
		aggregator.init(children, true, ((Number) ((LiteralExpression) children.get(2)).getValue()).intValue());

		return aggregator;
	}
}
