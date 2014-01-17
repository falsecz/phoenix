

package com.salesforce.phoenix.parse;

import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.function.Jebka;
import java.sql.SQLException;
import java.util.List;


public class JebkaAggregateParseNode extends DelegateConstantToCountParseNode {

    public JebkaAggregateParseNode(String name, List<ParseNode> children, FunctionParseNode.BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public Jebka create(List<Expression> children, StatementContext context) throws SQLException {
        return new Jebka(children, getDelegateFunction(children,context));
    }
}