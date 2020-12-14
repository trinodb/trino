/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.DoubleType;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.basicAggregation;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.expressionType;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.functionName;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.singleInput;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.variable;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static java.lang.String.format;

public class ImplementOracleStddev
        implements AggregateFunctionRule
{
    private static final List<String> STDDEV_FUNCTION_NAMES = ImmutableList.of("stddev", "stddev_samp");
    private static final Capture<Variable> INPUT = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().matching(name -> STDDEV_FUNCTION_NAMES.stream().anyMatch(name::equalsIgnoreCase)))
                .with(singleInput().matching(
                        variable()
                                .with(expressionType().matching(DoubleType.class::isInstance))
                                .capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(input.getName());
        verify(columnHandle.getColumnType().equals(DOUBLE));
        verify(aggregateFunction.getOutputType().equals(DOUBLE));

        // Oracle's stddev function has slightly different semantics to Presto's when a table has one or no records. Oracle returns zero while Presto return null.
        // The Oracle stddev_samp implementation matches Presto's semantics for both functions.
        return Optional.of(new JdbcExpression(
                format("stddev_samp(%s)", columnHandle.toSqlExpression(context.getIdentifierQuote())),
                columnHandle.getJdbcTypeHandle()));
    }
}
