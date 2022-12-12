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

import io.airlift.slice.Slice;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;

public class RewriteVarcharConstant
        implements ConnectorExpressionRule<Constant, String>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(VarcharType.class::isInstance));

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<String> rewrite(Constant constant, Captures captures, RewriteContext<String> context)
    {
        if (constant.getValue() == null) {
            return Optional.empty();
        }
        Slice slice = (Slice) constant.getValue();
        if (slice == null) {
            return Optional.empty();
        }

        String sliceUtf8String = slice.toStringUtf8();
        return Optional.of("'" + sliceUtf8String.replace("'", "''").replace("\\", "\\\\") + "'");
    }
}
