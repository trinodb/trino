/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.jdbc;

import com.starburstdata.trino.plugins.snowflake.SnowflakeSessionProperties;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;

public class RewriteJsonPath
        implements ConnectorExpressionRule<Constant, String>
{
    private final Pattern<Constant> pattern;

    public RewriteJsonPath(Type jsonPathType)
    {
        this.pattern = constant().with(type().matching(jsonPathType.getClass()::isInstance));
    }

    @Override
    public Pattern<Constant> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<String> rewrite(Constant constant, Captures captures, RewriteContext<String> context)
    {
        if (!SnowflakeSessionProperties.getExperimentalPushdownEnabled(context.getSession())) {
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            return Optional.empty();
        }
        String snowflakeJsonPath = constant.getValue().toString();
        if (snowflakeJsonPath == null) {
            return Optional.empty();
        }
        if (snowflakeJsonPath.startsWith("$.")) {
            snowflakeJsonPath = snowflakeJsonPath.substring("$.".length());
        }
        snowflakeJsonPath = "'" + snowflakeJsonPath.replace("'", "''") + "'";
        return Optional.of(snowflakeJsonPath);
    }
}
