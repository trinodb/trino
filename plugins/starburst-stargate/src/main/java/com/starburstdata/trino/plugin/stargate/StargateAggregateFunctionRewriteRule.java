/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

class StargateAggregateFunctionRewriteRule
        implements AggregateFunctionRule<JdbcExpression>
{
    private final Function<ConnectorSession, Set<String>> supportedFunctions;
    private final Function<Type, Optional<JdbcTypeHandle>> toTypeHandle;

    public StargateAggregateFunctionRewriteRule(Function<ConnectorSession, Set<String>> supportedFunctions, Function<Type, Optional<JdbcTypeHandle>> toTypeHandle)
    {
        this.supportedFunctions = requireNonNull(supportedFunctions, "supportedFunctions is null");
        this.toTypeHandle = requireNonNull(toTypeHandle, "toTypeHandle is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return Pattern.typeOf(AggregateFunction.class);
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        String functionName = aggregateFunction.getFunctionName();
        if (!supportedFunctions.apply(context.getSession()).contains(functionName)) {
            // TODO test
            return Optional.empty();
        }

        Optional<JdbcTypeHandle> outputTypeHandle = toTypeHandle.apply(aggregateFunction.getOutputType());
        if (outputTypeHandle.isEmpty()) {
            // Aggregation function may return a result that we currently cannot support on read
            return Optional.empty();
        }

        List<Optional<String>> mappedInputs = aggregateFunction.getInputs().stream()
                .map(input -> toSql(context, input))
                .collect(toImmutableList());
        if (mappedInputs.stream().anyMatch(Optional::isEmpty)) {
            // TODO support complex ConnectorExpressions
            return Optional.empty();
        }
        String arguments = mappedInputs.stream()
                .map(Optional::get)
                .collect(joining(", "));

        String orderBy = aggregateFunction.getSortItems().stream()
                .map(sortItem -> {
                    JdbcColumnHandle columnHandle = getAssignment(context, sortItem.getName());
                    return format("%s %s",
                            context.getIdentifierQuote().apply(columnHandle.getColumnName()),
                            toSql(sortItem.getSortOrder()));
                })
                .collect(joining(", "));

        Optional<String> filter;
        if (aggregateFunction.getFilter().isPresent()) {
            filter = toSql(context, aggregateFunction.getFilter().get());
            if (filter.isEmpty()) {
                // TODO support complex ConnectorExpressions
                return Optional.empty();
            }
        }
        else {
            filter = Optional.empty();
        }

        String call = functionName + "(";
        if (aggregateFunction.isDistinct()) {
            call += "DISTINCT ";
        }
        call += arguments;
        if (!orderBy.isEmpty()) {
            call += " ORDER BY " + orderBy;
        }
        call += ")";
        if (filter.isPresent()) {
            call += " FILTER (WHERE " + filter.get() + ")";
        }

        return Optional.of(new JdbcExpression(call, outputTypeHandle.get()));
    }

    private Optional<String> toSql(RewriteContext context, ConnectorExpression expression)
    {
        if (!(expression instanceof Variable)) {
            // TODO support complex ConnectorExpressions
            return Optional.empty();
        }

        JdbcColumnHandle columnHandle = getAssignment(context, ((Variable) expression).getName());
        String sqlExpression = context.getIdentifierQuote().apply(columnHandle.getColumnName());
        verify(!sqlExpression.isBlank(), "Blank sqlExpression [%s] for %s", sqlExpression, columnHandle);
        return Optional.of(sqlExpression);
    }

    private static String toSql(SortOrder sortOrder)
    {
        switch (requireNonNull(sortOrder, "sortOrder is null")) {
            case ASC_NULLS_FIRST:
                return "ASC NULLS FIRST";
            case ASC_NULLS_LAST:
                return "ASC NULLS LAST";
            case DESC_NULLS_FIRST:
                return "DESC NULLS FIRST";
            case DESC_NULLS_LAST:
                return "DESC NULLS LAST";
        }
        throw new UnsupportedOperationException("Unknown SortOrder: " + sortOrder);
    }

    // TODO simplify with https://github.com/trinodb/trino/pull/6125
    private static JdbcColumnHandle getAssignment(RewriteContext context, String name)
    {
        requireNonNull(name, "name is null");
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignments().get(name);
        verifyNotNull(columnHandle, "Unbound variable: %s", name);
        return columnHandle;
    }
}
