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
import io.trino.plugin.base.expression.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BigintType;

import java.sql.Types;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.singleInput;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.variable;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.String.format;

public class ImplementAvgBigint
        implements AggregateFunctionRule<JdbcExpression>
{
    private static final Capture<Variable> INPUT = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("avg"))
                .with(singleInput().matching(
                        variable()
                                .with(expressionType().matching(BigintType.class::isInstance))
                                .capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(input.getName());
        verify(aggregateFunction.getOutputType() == DOUBLE);

        return Optional.of(new JdbcExpression(
                format("avg(CAST((%s) AS double))", context.getIdentifierQuote().apply(columnHandle.getColumnName())),
                new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), 0, 0, Optional.empty(), Optional.empty())));
    }
}
