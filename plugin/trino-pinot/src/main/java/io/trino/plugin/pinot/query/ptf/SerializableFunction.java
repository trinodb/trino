package io.trino.plugin.pinot.query.ptf;

import org.apache.pinot.common.request.Function;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public record SerializableFunction(String operator, List<SerializableExpression> arguments)
{
    public SerializableFunction
    {
        requireNonNull(operator, "operator is null");
        requireNonNull(arguments, "arguments is null");
    }

    public SerializableFunction(Function function)
    {
        this(function.getOperator(), function.getOperands().stream()
                .map(SerializableExpression::new)
                .collect(toImmutableList()));
    }
    public Function toFunction()
    {
        Function function = new Function(operator);
        function.setOperands(arguments.stream()
                .map(SerializableExpression::toExpression)
                .collect(toImmutableList()));
        return function;
    }
}
