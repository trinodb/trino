/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce.testing.datatype;

import com.starburstdata.presto.plugin.salesforce.testing.sql.SalesforceTestTable;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.List;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

// Extension of CreateAsSelectDataSetup to use our SalesforceTestTable
public class SalesforceCreateAsSelectDataSetup
        extends CreateAsSelectDataSetup
{
    private final SqlExecutor sqlExecutor;
    private final String tableNamePrefix;

    public SalesforceCreateAsSelectDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        super(sqlExecutor, tableNamePrefix);
        this.sqlExecutor = sqlExecutor;
        this.tableNamePrefix = tableNamePrefix;
    }

    @Override
    public TestTable setupTestTable(List<ColumnSetup> inputs)
    {
        List<String> columnValues = inputs.stream()
                .map(this::format)
                .collect(toList());
        String selectBody = range(0, columnValues.size())
                .mapToObj(i -> String.format("%s col_%d", columnValues.get(i), i))
                .collect(joining(",\n"));
        return new SalesforceTestTable(sqlExecutor, tableNamePrefix, "AS SELECT " + selectBody);
    }

    private String format(ColumnSetup input)
    {
        if (input.getDeclaredType().isEmpty()) {
            return input.getInputLiteral();
        }
        return String.format(
                "CAST(%s AS %s)",
                input.getInputLiteral(),
                input.getDeclaredType().get());
    }
}
