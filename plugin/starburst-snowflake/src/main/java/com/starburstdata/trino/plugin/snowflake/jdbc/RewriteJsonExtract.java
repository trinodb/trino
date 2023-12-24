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
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.type.Type;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static java.lang.String.format;

public class RewriteJsonExtract
        extends AbstractRewriteJsonExtract
{
    public RewriteJsonExtract(Type jsonType)
    {
        super("json_extract", type().equalTo(jsonType));
    }

    @Override
    public ParameterizedExpression rewriteExpression(ParameterizedExpression valueExpression, ParameterizedExpression jsonExpression)
    {
        return new ParameterizedExpression(
                format("GET_PATH((%s), (%s))", valueExpression.expression(), jsonExpression.expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(valueExpression.parameters())
                        .addAll(rewriteJsonPath(jsonExpression.parameters()))
                        .build());
    }
}
