/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithMemory;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

// Extra Presto connector tests which require enabled writes and do not fit
// in TestPrestoConnectorDistributedQueriesWritesEnabled.
public class TestPrestoConnectorWithWritesEnabledExtraTests
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithMemory(
                Map.of(),
                TpchTable.getTables()));
        return createPrestoConnectorQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "memory"),
                        "allow-drop-table", "true"));
    }

    @Test(dataProvider = "largeInValuesCount", dataProviderClass = AbstractTestDistributedQueries.class)
    public void testLargeInLongColumnName(int valuesCount)
    {
        String tableName = "test_large_column_name_" + randomTableSuffix();
        String columnName = "this_is_a_very_looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong_column_name";
        assertUpdate(format("CREATE TABLE %s (%s bigint)", tableName, columnName));

        String longValues = range(0, valuesCount)
                .mapToObj(Integer::toString)
                .collect(joining(", "));

        assertQuery(format("SELECT * FROM %s WHERE %s IN (%s)", tableName, columnName, longValues), "SELECT 1 WHERE 1=2");
        assertQuery(format("SELECT * FROM %s WHERE %s NOT IN (%s)", tableName, columnName, longValues), "SELECT 1 WHERE 1=2");

        assertQuery(format("SELECT * FROM %s WHERE %s IN (mod(1000, %s), %s)", tableName, columnName, columnName, longValues), "SELECT 1 WHERE 1=2");
        assertQuery(format("SELECT * FROM %s WHERE %s NOT IN (mod(1000, %s), %s)", tableName, columnName, columnName, longValues), "SELECT 1 WHERE 1=2");
    }
}
