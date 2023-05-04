/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import io.airlift.log.Logger;
import io.trino.testng.services.ManageTestResources;
import io.trino.tpch.TpchTable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.TEST_SCHEMA;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class TestSynapseInstanceCleaner
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    private SynapseServer synapseServer;

    private static final int ERROR_OBJECT_NOT_FOUND = 3701;

    private static final Logger LOG = Logger.get(TestSynapseInstanceCleaner.class);
    private static final String TABLE_OF_OBJECTS = "sys.objects";
    private static final String TABLE_OF_TABLES = "sys.tables";
    private static final String TABLE_OF_VIEWS = "sys.views";

    private static final Collection<String> TABLES_TO_KEEP =
            TpchTable.getTables().stream()
                    .map(TpchTable::getTableName)
                    .map(s -> s.toLowerCase(Locale.ENGLISH))
                    .collect(toUnmodifiableSet());

    @BeforeClass
    public void setUp()
    {
        synapseServer = new SynapseServer();
    }

    @Test
    public void synapseInstanceCleaner()
    {
        logObjectsCount();
        LOG.info("Identifying objects to drop...");
        if (!TABLES_TO_KEEP.isEmpty()) {
            LOG.info("Never drop these tables: %s", join(", ", TABLES_TO_KEEP));
        }

        // Drop all tables and views created before yesterday (in UTC)
        // Note: because of how DATEDIFF counts, it really is "before yesterday," and not "over 1 day ago."
        Collection<String> tablesToDrop = getStringColumn(
                "Name",
                TABLE_OF_TABLES,
                "DATEDIFF(day, create_date, GETUTCDATE()) > 1").stream()
                .map(tableName -> tableName.toLowerCase(Locale.ENGLISH))
                .filter(not(TABLES_TO_KEEP::contains))
                .collect(toUnmodifiableSet());

        Collection<String> viewsToDrop = getStringColumn(
                "Name",
                TABLE_OF_VIEWS,
                "DATEDIFF(day, create_date, GETUTCDATE()) > 1 AND Name != 'user_context'");

        if (tablesToDrop.isEmpty()) {
            LOG.info("Not dropping any tables.");
        }
        if (viewsToDrop.isEmpty()) {
            LOG.info("Not dropping any views.");
        }
        LOG.info("Identified %d tables to drop.", tablesToDrop.size());
        LOG.info("Tables to drop: %s", tablesToDrop);
        LOG.info("Identified %d views to drop.", viewsToDrop.size());
        LOG.info("Views to drop: %s", viewsToDrop);
        // Azure Synapse does not support "DROP TABLE IF EXISTS"
        try {
            tablesToDrop.stream()
                    .forEach(tableName -> synapseServer.execute(format("DROP TABLE %s.[%s]", TEST_SCHEMA, tableName)));
            viewsToDrop.stream()
                    .forEach(viewName -> synapseServer.execute(format("DROP VIEW %s.[%s]", TEST_SCHEMA, viewName)));
        }
        catch (RuntimeException e) {
            if (e.getCause() instanceof SQLServerException && ((SQLServerException) e.getCause()).getErrorCode() != ERROR_OBJECT_NOT_FOUND) {
                throw e;
            }
        }
        logObjectsCount();
    }

    private int getRowCount(String tableName)
    {
        return synapseServer.executeQuery("SELECT count(*) FROM " + tableName, resultSet -> {
            try {
                resultSet.next();
                return resultSet.getInt(1);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Collection<String> getStringColumn(String column, String table, String where)
    {
        Collection<String> results = new ArrayList<>();
        return synapseServer.executeQuery(format("SELECT %s FROM %s WHERE %s", column, table, where), resultSet -> {
            try {
                while (resultSet.next()) {
                    results.add(resultSet.getString(column));
                }
                return results;
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Log total number of objects in the test schema.
     */
    private void logObjectsCount()
    {
        int tableCount = getRowCount(TABLE_OF_OBJECTS);
        LOG.info("Schema '%s' contains %d objects.", TEST_SCHEMA, tableCount);
    }
}
