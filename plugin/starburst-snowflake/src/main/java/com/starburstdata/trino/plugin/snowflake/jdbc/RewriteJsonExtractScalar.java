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
import io.trino.spi.type.VarcharType;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static java.lang.String.format;

public class RewriteJsonExtractScalar
        extends AbstractRewriteJsonExtract
{
    public RewriteJsonExtractScalar()
    {
        super("json_extract_scalar", type().matching(VarcharType.class::isInstance));
    }

    @Override
    public ParameterizedExpression rewriteExpression(ParameterizedExpression valueExpression, ParameterizedExpression jsonExpression)
    {
        // For non-scalar types Trino's json_extract_scalar returns NULL while Snowflake's JSON_EXTRACT_PATH_TEXT returns the JSON at given path as a string
        return new ParameterizedExpression(
                format("""
                                CASE TYPEOF(GET_PATH((%s), (%s)))
                                    WHEN 'ARRAY' THEN NULL
                                    WHEN 'OBJECT' THEN NULL
                                    ELSE JSON_EXTRACT_PATH_TEXT((%s), (%s))
                                END""",
                        valueExpression.expression(), jsonExpression.expression(),
                        valueExpression.expression(), jsonExpression.expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(valueExpression.parameters())
                        .addAll(rewriteJsonPath(jsonExpression.parameters()))
                        .addAll(valueExpression.parameters())
                        .addAll(rewriteJsonPath(jsonExpression.parameters()))
                        .build());
    }
}
