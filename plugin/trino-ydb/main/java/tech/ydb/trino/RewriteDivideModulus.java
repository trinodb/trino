package tech.ydb.trino;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.expression.StandardFunctions.DIVIDE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MODULO_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

/**
 * Rewrite <code>$divide(a, b)</code>, <code>$modulus(a, b)</code> as <code>a / b</code>, <code>a % b</code>,
 * for integral operands with a non-zero constant divisor other than -1.
 * <br /> <br />
 * This is because YDB, unlike Trino, suppresses arithmetic errors (including division by zero), and any
 * non-constant expression <i>may</i> evaluate to zero and cause a different result if pushed down to YDB.
 * YQL also returns NULL for signed-minimum modulo -1, while Trino returns zero.
 */
public class RewriteDivideModulus implements ConnectorExpressionRule<Call, ParameterizedExpression> {
    private final Pattern<Call> PATTERN;

    public RewriteDivideModulus() {
        this.PATTERN = call()
                .with(functionName().matching(name -> name.equals(DIVIDE_FUNCTION_NAME) || name.equals(MODULO_FUNCTION_NAME)))
                .with(argumentCount().matching(count -> count == 2))
                .with(type().matching(resultType -> resultType.equals(TINYINT) || resultType.equals(SMALLINT) ||
                        resultType.equals(INTEGER) || resultType.equals(BIGINT)))
                .matching((Call call, RewriteContext<ParameterizedExpression> _) ->
                        call.getArguments().stream().noneMatch(arg -> arg instanceof Call));
    }

    @Override
    public Pattern<Call> getPattern() {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context) {
        if (!(call.getArguments().get(1) instanceof Constant rightConstant) ||
                !(rightConstant.getValue() instanceof Number number) ||
                number.longValue() == 0 || number.longValue() == -1) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> left = context.defaultRewrite(call.getArguments().getFirst());
        if (left.isEmpty()) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> right = context.defaultRewrite(call.getArguments().get(1));
        if (right.isEmpty()) {
            return Optional.empty();
        }
        String operator = call.getFunctionName().equals(DIVIDE_FUNCTION_NAME) ? "/" : "%";
        return Optional.of(new ParameterizedExpression(
                format("(%s) %s (%s)", left.get().expression(), operator, right.get().expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(left.get().parameters())
                        .addAll(right.get().parameters())
                        .build()));
    }

}
