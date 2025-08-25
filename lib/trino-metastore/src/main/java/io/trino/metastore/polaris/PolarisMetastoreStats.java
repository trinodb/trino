/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.metastore.polaris;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

/**
 * Statistics tracking for Polaris metastore operations.
 * Used for monitoring and testing Polaris API call patterns.
 */
public class PolarisMetastoreStats
{
    private final CounterStat createDatabase = new CounterStat();
    private final CounterStat getDatabase = new CounterStat();
    private final CounterStat getAllDatabases = new CounterStat();

    private final CounterStat createIcebergTable = new CounterStat();
    private final CounterStat loadIcebergTable = new CounterStat();
    private final CounterStat dropIcebergTable = new CounterStat();
    private final CounterStat listIcebergTables = new CounterStat();

    private final CounterStat createGenericTable = new CounterStat();
    private final CounterStat loadGenericTable = new CounterStat();
    private final CounterStat dropGenericTable = new CounterStat();
    private final CounterStat listGenericTables = new CounterStat();

    private final CounterStat getTable = new CounterStat();
    private final CounterStat getAllTables = new CounterStat();
    private final CounterStat createTable = new CounterStat();
    private final CounterStat dropTable = new CounterStat();
    private final CounterStat replaceTable = new CounterStat();
    private final CounterStat replaceIcebergTable = new CounterStat();
    private final CounterStat replaceGenericTable = new CounterStat();

    @Managed
    @Nested
    public CounterStat getCreateDatabase()
    {
        return createDatabase;
    }

    @Managed
    @Nested
    public CounterStat getGetDatabase()
    {
        return getDatabase;
    }

    @Managed
    @Nested
    public CounterStat getGetAllDatabases()
    {
        return getAllDatabases;
    }

    @Managed
    @Nested
    public CounterStat getCreateIcebergTable()
    {
        return createIcebergTable;
    }

    @Managed
    @Nested
    public CounterStat getLoadIcebergTable()
    {
        return loadIcebergTable;
    }

    @Managed
    @Nested
    public CounterStat getDropIcebergTable()
    {
        return dropIcebergTable;
    }

    @Managed
    @Nested
    public CounterStat getListIcebergTables()
    {
        return listIcebergTables;
    }

    @Managed
    @Nested
    public CounterStat getCreateGenericTable()
    {
        return createGenericTable;
    }

    @Managed
    @Nested
    public CounterStat getLoadGenericTable()
    {
        return loadGenericTable;
    }

    @Managed
    @Nested
    public CounterStat getDropGenericTable()
    {
        return dropGenericTable;
    }

    @Managed
    @Nested
    public CounterStat getListGenericTables()
    {
        return listGenericTables;
    }

    @Managed
    @Nested
    public CounterStat getGetTable()
    {
        return getTable;
    }

    @Managed
    @Nested
    public CounterStat getGetAllTables()
    {
        return getAllTables;
    }

    @Managed
    @Nested
    public CounterStat getCreateTable()
    {
        return createTable;
    }

    @Managed
    @Nested
    public CounterStat getDropTable()
    {
        return dropTable;
    }

    @Managed
    @Nested
    public CounterStat getReplaceTable()
    {
        return replaceTable;
    }

    @Managed
    @Nested
    public CounterStat getReplaceIcebergTable()
    {
        return replaceIcebergTable;
    }

    @Managed
    @Nested
    public CounterStat getReplaceGenericTable()
    {
        return replaceGenericTable;
    }
}
