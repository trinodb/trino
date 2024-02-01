/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import static java.lang.String.format;

public class TestSapHanaSimpleTableStatistics
        extends AbstractTestSapHanaTableStatistics
{
    @Override
    protected void gatherStats(String tableName)
    {
        String schemaTableName = format("%s.%s", getSession().getSchema().orElseThrow(), tableName);
        sapHanaServer.execute("CREATE STATISTICS ON " + schemaTableName + " TYPE SIMPLE");
        // MERGE DELTA is required to force a stats refresh and ensure stats are up to date.
        // If not invoked explicitly, MERGE DELTA happens periodically about every 2 minutes, unless disabled for a table.
        // If disabled, stats are never updated automatically.
        sapHanaServer.execute("MERGE DELTA OF " + schemaTableName + " WITH PARAMETERS ('FORCED_MERGE' = 'ON')");
    }
}
