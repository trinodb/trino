/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce.testing.sql;

import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

// Extension of TestTable to be able to append the __c suffix dropping the table
// One of the TestTable constructors also inserts data, but that is unused in these tests
// Note that it won't work for Salesforce and we are unable to override this behavior since it occurs in the constructor
public class SalesforceTestTable
        extends TestTable
{
    private final SqlExecutor sqlExecutor;

    public SalesforceTestTable(SqlExecutor sqlExecutor, String namePrefix, String tableDefinition)
    {
        super(sqlExecutor, namePrefix, tableDefinition);
        this.sqlExecutor = sqlExecutor;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP TABLE " + getName() + "__c");
    }
}
