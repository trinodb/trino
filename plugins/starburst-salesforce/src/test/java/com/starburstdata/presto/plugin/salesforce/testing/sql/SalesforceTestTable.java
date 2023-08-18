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

import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

// Extension of TestTable to be able to create the table without a suffix and truncate data from the table on close
public class SalesforceTestTable
        extends TestTable
{
    private final String jdbcUrl;
    private final String tableName;

    public SalesforceTestTable(String jdbcUrl, String tableName, String tableDefinition)
    {
        // Pass in a no-op SqlExecutor as the base class attempts to create the table, but we want to override that functionality here
        super(sql -> {}, tableName, tableDefinition);

        this.jdbcUrl = requireNonNull(jdbcUrl, "sqlExecutor is null");
        this.tableName = requireNonNull(tableName, "tableName is null");

        JdbcSqlExecutor sqlExecutor = new JdbcSqlExecutor(jdbcUrl);

        // Salesforce has limits on custom objects and tables are soft-deleted and then actually deleted weeks later
        // We've had issues with CI builds where we fill up the quota and then the builds fail
        // Instead we will create the table if it doesn't exist and truncate it to ensure it is empty
        // We can't use CREATE TABLE IF NOT EXISTS, an error is thrown because the table exists with a __c suffix and the
        // driver will try to create it anyway
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery(format("SELECT COUNT(*) FROM sys_tables WHERE TableName = '%s__c'", tableName))) {
            boolean exists = false;
            while (results.next()) {
                exists = results.getInt(1) == 1;
            }

            if (!exists) {
                sqlExecutor.execute(format("CREATE TABLE %s %s", tableName, tableDefinition));
                sqlExecutor.execute("RESET SCHEMA CACHE");
            }
            else {
                truncate();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error checking if table exists table", e);
        }
    }

    @Override
    public String getName()
    {
        // Override to return table name without random suffix
        return tableName;
    }

    @Override
    public void close()
    {
        // Truncate the table instead of deleting it to avoid hitting object limits in Salesforce
        truncate();
    }

    private void truncate()
    {
        // Salesforce driver does not support SQL TRUNCATE
        // Instead we delete records by querying for the 'Id' column of the table (a Salesforce column) and then inserting
        // into a temp table which is stored in the driver. We then delete from the main table by selecting from the temp table
        // and query the table to assert that the table is empty
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                PreparedStatement preparedStatement = connection.prepareStatement(format("INSERT INTO %s__c#TEMP (Id) VALUES (?)", tableName));
                ResultSet results = statement.executeQuery(format("SELECT Id FROM %s__c", tableName))) {
            boolean hasData = false;
            while (results.next()) {
                hasData = true;
                preparedStatement.setObject(1, results.getObject(1));
                preparedStatement.execute();
            }

            if (hasData) {
                statement.execute(format("DELETE FROM %s__c WHERE EXISTS SELECT Id FROM %s__c#TEMP", tableName, tableName));
            }

            try (ResultSet countResults = statement.executeQuery(format("SELECT COUNT(*) FROM %s__c", tableName))) {
                countResults.next();
                int numRows = countResults.getInt(1);
                if (numRows != 0) {
                    throw new RuntimeException(format("Table %s has %s rows but expected 0", tableName, numRows));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error truncating table", e);
        }
    }
}
