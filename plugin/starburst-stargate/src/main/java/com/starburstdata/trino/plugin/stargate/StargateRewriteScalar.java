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

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StargateRewriteScalar
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Pattern<Call> PATTERN = call();

    private final Function<ConnectorSession, Set<String>> supportedFunctions;

    public StargateRewriteScalar(Function<ConnectorSession, Set<String>> supportedFunctions)
    {
        this.supportedFunctions = requireNonNull(supportedFunctions, "supportedFunctions is null");
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        if (call.getFunctionName().getCatalogSchema().isPresent()) {
            // TODO support qualified function names
            return Optional.empty();
        }
        String functionName = call.getFunctionName().getName();

        Set<String> supportedFunctions = this.supportedFunctions.apply(context.getSession());
        if (!supportedFunctions.contains(functionName)) {
            return Optional.empty();
        }

        ImmutableList.Builder<QueryParameter> parameters = ImmutableList.builder();
        List<ConnectorExpression> arguments = call.getArguments();
        List<String> rewrittenArguments = new ArrayList<>(arguments.size());
        for (ConnectorExpression argument : arguments) {
            Optional<ParameterizedExpression> rewritten = context.defaultRewrite(argument);
            if (rewritten.isEmpty()) {
                return Optional.empty();
            }
            rewrittenArguments.add(rewritten.get().expression());
            parameters.addAll(rewritten.get().parameters());
        }

        return Optional.of(new ParameterizedExpression(
                format("%s(%s)", functionName, String.join(", ", rewrittenArguments)),
                parameters.build()));
    }
}
