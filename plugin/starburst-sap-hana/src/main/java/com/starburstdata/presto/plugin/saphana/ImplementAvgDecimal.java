/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DecimalType;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.saphana.SapHanaClient.SAP_HANA_MAX_DECIMAL_PRECISION;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleArgument;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.variable;
import static java.lang.String.format;

public class ImplementAvgDecimal
        implements AggregateFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Variable> INPUT = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("avg"))
                .with(singleArgument().matching(
                        variable()
                                .with(expressionType().matching(DecimalType.class::isInstance))
                                .capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(input.getName());
        DecimalType type = (DecimalType) columnHandle.getColumnType();
        verify(aggregateFunction.getOutputType().equals(type));

        ParameterizedExpression rewrittenArgument = context.rewriteExpression(input).orElseThrow();

        // When decimal type has maximum precision we can get result that does not match Trino avg semantics.
        if (type.getPrecision() == SAP_HANA_MAX_DECIMAL_PRECISION) {
            return Optional.of(new JdbcExpression(
                    format("avg(CAST(%s AS decimal(%s, %s)))", rewrittenArgument.expression(), type.getPrecision(), type.getScale()),
                    rewrittenArgument.parameters(),
                    columnHandle.getJdbcTypeHandle()));
        }

        // SAP HANA avg function rounds down resulting decimal.
        // To match Trino avg semantics, we extend scale by 1 and round result to target scale.
        return Optional.of(new JdbcExpression(
                format("round(avg(CAST(%s AS decimal(%s, %s))), %s)", rewrittenArgument.expression(), type.getPrecision() + 1, type.getScale() + 1, type.getScale()),
                rewrittenArgument.parameters(),
                columnHandle.getJdbcTypeHandle()));
    }
}
