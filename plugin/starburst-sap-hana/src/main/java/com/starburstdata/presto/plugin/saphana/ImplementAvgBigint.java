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

import io.trino.plugin.jdbc.aggregation.BaseImplementAvgBigint;

public class ImplementAvgBigint
        extends BaseImplementAvgBigint
{
    @Override
    protected String getRewriteFormatExpression()
    {
        return "avg(CAST((%s) AS double))";
    }
}
