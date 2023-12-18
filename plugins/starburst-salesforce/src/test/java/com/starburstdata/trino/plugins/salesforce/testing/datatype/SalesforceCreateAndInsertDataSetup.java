/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.salesforce.testing.datatype;

import com.starburstdata.trino.plugins.salesforce.testing.sql.SalesforceTestTable;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.List;
import java.util.stream.IntStream;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

// Extension of CreateAndInsertDataSetup to add __c suffixes
public class SalesforceCreateAndInsertDataSetup
        extends CreateAndInsertDataSetup
{
    private final SqlExecutor sqlExecutor;
    private final String jdbcUrl;
    private final String tableName;

    public SalesforceCreateAndInsertDataSetup(SqlExecutor sqlExecutor, String jdbcUrl, String tableName)
    {
        super(sqlExecutor, tableName);
        this.sqlExecutor = sqlExecutor;
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
    }

    @Override
    public TestTable setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        TestTable testTable = createTestTable(inputs);
        try {
            sqlExecutor.execute("RESET SCHEMA CACHE");
            insertRows(testTable, inputs);
        }
        catch (Exception e) {
            closeAllSuppress(e, testTable);
            throw e;
        }
        return testTable;
    }

    private void insertRows(TestTable testTable, List<ColumnSetup> inputs)
    {
        String valueLiterals = inputs.stream()
                .map(ColumnSetup::getInputLiteral)
                .collect(joining(", "));

        // Build a list of columns otherwise they will end up as Salesforce does not respect the column order in the DDL when running select *
        String columns = IntStream.range(0, inputs.size())
                .mapToObj(column -> format("col_%d__c", column))
                .collect(joining(", "));

        sqlExecutor.execute(format("INSERT INTO %s__c (%s) VALUES(%s)", testTable.getName(), columns, valueLiterals));
    }

    private TestTable createTestTable(List<ColumnSetup> inputs)
    {
        return new SalesforceTestTable(jdbcUrl, tableName, tableDefinition(inputs));
    }
}
