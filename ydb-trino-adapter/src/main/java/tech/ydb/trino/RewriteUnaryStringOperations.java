package tech.ydb.trino;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;

public class RewriteUnaryStringOperations implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression> {
    private static final Capture<ConnectorExpression> VALUE = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().matching(name -> name.equals(new FunctionName("upper")) ||
                    name.equals(new FunctionName("lower"))))
            .with(type().matching(type -> type instanceof VarcharType))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(expression().capturedAs(VALUE)));

    @Override
    public Pattern<? extends ConnectorExpression> getPattern() {
        return PATTERN;
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context) {
        JdbcTypeHandle varcharTypeHandle = YdbTypeUtils.toTypeHandle(VarcharType.VARCHAR).orElseThrow();
        Call call = (Call) projectionExpression;
        Optional<ParameterizedExpression> rewrittenValue = context.rewriteExpression(captures.get(VALUE));
        if (rewrittenValue.isEmpty()) {
            return Optional.empty();
        }

        String expression = mapToYql(call.getFunctionName().getName()).formatted(rewrittenValue.get().expression());

        return Optional.of(new JdbcExpression(
                expression,
                ImmutableList.copyOf(rewrittenValue.get().parameters()),
                varcharTypeHandle
        ));
    }

    private static String mapToYql(String functionName) {
        return switch (functionName) {
            case "upper" -> "Unicode::ToUpper(%s)";
            case "lower" -> "Unicode::ToLower(%s)";
            default -> throw new IllegalArgumentException("Unexpected function name: " + functionName);
        };
    }
}
