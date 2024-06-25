package io.trino.plugin.pinot.query.ptf;

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Identifier;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record SerializableExpression(ExpressionType expressionType, Optional<String> identifier, Optional<SerializableFunction> function, Optional<SerializableLiteral> literal)
{
    public SerializableExpression
    {
        requireNonNull(expressionType, "expressionType is null");
        requireNonNull(identifier, "identifier is null");
        requireNonNull(function, "function is null");
        requireNonNull(literal, "literal is null");
    }

    public SerializableExpression(Expression expression)
    {
        this(
                expression.getType(),
                Optional.ofNullable(expression.getIdentifier()).map(Identifier::getName),
                Optional.ofNullable(expression.getFunctionCall()).map(SerializableFunction::new),
                Optional.ofNullable(expression.getLiteral()).map(SerializableLiteral::new));
    }

    public Expression toExpression()
    {
        Expression expression = new Expression(expressionType);
        switch (expressionType) {
            case LITERAL -> literal.map(SerializableLiteral::toLiteral).ifPresent(expression::setLiteral);
            case IDENTIFIER -> identifier.map(Identifier::new).ifPresent(expression::setIdentifier);
            case FUNCTION -> function.map(SerializableFunction::toFunction).ifPresent(expression::setFunctionCall);
        }
        return expression;
    }
}
