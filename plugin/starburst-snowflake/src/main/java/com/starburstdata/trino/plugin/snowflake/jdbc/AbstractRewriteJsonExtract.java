/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.snowflake.SnowflakeSessionProperties;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.matching.PropertyPattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;

public abstract class AbstractRewriteJsonExtract
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private static final Capture<ConnectorExpression> JSON_PATH = newCapture();
    private final Pattern<Call> pattern;

    protected AbstractRewriteJsonExtract(String functionName, PropertyPattern<? super Call, ?, ?> parameterTypeCheck)
    {
        this.pattern = call()
                .with(functionName().equalTo(new FunctionName(functionName)))
                .with(parameterTypeCheck)
                .with(argumentCount().equalTo(2))
                .with(argument(0).matching(expression().capturedAs(VALUE)))
                // this only captures cases where the JSON_PATH is a literal and no CAST is involved
                .with(argument(1).matching(expression().capturedAs(JSON_PATH)));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        if (!SnowflakeSessionProperties.getExperimentalPushdownEnabled(context.getSession())) {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> value = context.defaultRewrite(captures.get(VALUE));
        if (value.isEmpty()) {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> jsonPath = context.defaultRewrite(captures.get(JSON_PATH));
        return jsonPath.map(parameterizedExpression -> rewriteExpression(value.get(), parameterizedExpression));
    }

    abstract ParameterizedExpression rewriteExpression(ParameterizedExpression valueExpression, ParameterizedExpression jsonExpression);

    protected List<QueryParameter> rewriteJsonPath(List<QueryParameter> parameters)
    {
        verify(parameters.size() == 1, "Expected a single JSON path parameter");
        QueryParameter queryParameter = parameters.get(0);
        Optional<Object> parameterValue = queryParameter.getValue();

        verify(parameterValue.isPresent(), "Expected JSON path to be present");
        String jsonPath = ((Slice) parameterValue.get()).toStringUtf8();
        if (jsonPath.startsWith("$.")) {
            jsonPath = jsonPath.substring("$.".length());
        }

        return ImmutableList.of(new QueryParameter(queryParameter.getType(), Optional.of(Slices.utf8Slice(jsonPath))));
    }
}
