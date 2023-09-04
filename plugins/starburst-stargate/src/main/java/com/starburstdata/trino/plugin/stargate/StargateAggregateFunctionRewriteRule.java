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
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

class StargateAggregateFunctionRewriteRule
        implements AggregateFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private final Function<ConnectorSession, Set<String>> supportedFunctions;
    private final Function<Type, Optional<JdbcTypeHandle>> toTypeHandle;
    private final Function<String, String> identifierQuote;

    public StargateAggregateFunctionRewriteRule(
            Function<ConnectorSession, Set<String>> supportedFunctions,
            Function<Type, Optional<JdbcTypeHandle>> toTypeHandle,
            Function<String, String> identifierQuote)
    {
        this.supportedFunctions = requireNonNull(supportedFunctions, "supportedFunctions is null");
        this.toTypeHandle = requireNonNull(toTypeHandle, "toTypeHandle is null");
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return Pattern.typeOf(AggregateFunction.class);
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext<ParameterizedExpression> context)
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

        List<Optional<ParameterizedExpression>> rewrittenArguments = aggregateFunction.getArguments().stream()
                .map(context::rewriteExpression)
                .collect(toImmutableList());
        if (rewrittenArguments.stream().anyMatch(Optional::isEmpty)) {
            // TODO support complex ConnectorExpressions
            return Optional.empty();
        }
        String arguments = rewrittenArguments.stream()
                                   .map(Optional::get)
                                   .map(ParameterizedExpression::expression)
                                   .collect(joining(", "));
        List<QueryParameter> parameters = rewrittenArguments.stream()
                .map(Optional::get)
                .flatMap(parameterizedExpression -> parameterizedExpression.parameters().stream())
                .collect(toImmutableList());

        String orderBy = aggregateFunction.getSortItems().stream()
                .map(sortItem -> {
                    JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(sortItem.getName());
                    return format("%s %s",
                            identifierQuote.apply(columnHandle.getColumnName()),
                            toSql(sortItem.getSortOrder()));
                })
                .collect(joining(", "));

        Optional<ParameterizedExpression> filter = Optional.empty();
        if (aggregateFunction.getFilter().isPresent()) {
            filter = context.rewriteExpression(aggregateFunction.getFilter().get());
            if (filter.isEmpty()) {
                // TODO support complex ConnectorExpressions
                return Optional.empty();
            }
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
            call += " FILTER (WHERE " + filter.get().expression() + ")";
        }

        return Optional.of(new JdbcExpression(
                        call,
                        parameters,
                        outputTypeHandle.get()));
    }

    private static String toSql(SortOrder sortOrder)
    {
        return switch (requireNonNull(sortOrder, "sortOrder is null")) {
            case ASC_NULLS_FIRST -> "ASC NULLS FIRST";
            case ASC_NULLS_LAST -> "ASC NULLS LAST";
            case DESC_NULLS_FIRST -> "DESC NULLS FIRST";
            case DESC_NULLS_LAST -> "DESC NULLS LAST";
        };
    }
}
