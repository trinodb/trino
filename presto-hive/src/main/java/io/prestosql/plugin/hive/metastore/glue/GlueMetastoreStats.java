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
package io.prestosql.plugin.hive.metastore.glue;

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class GlueMetastoreStats
{
    private final GlueMetastoreApiStats getAllDatabases = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getDatabase = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getAllTables = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getAllViews = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats createDatabase = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats dropDatabase = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats renameDatabase = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats createTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats dropTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats replaceTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getPartitionNames = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getPartitions = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getPartition = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getPartitionByName = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats addPartitions = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats dropPartition = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats alterPartition = new GlueMetastoreApiStats();

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetAllDatabases()
    {
        return getAllDatabases;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetDatabase()
    {
        return getDatabase;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetAllTables()
    {
        return getAllTables;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetTable()
    {
        return getTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetAllViews()
    {
        return getAllViews;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getCreateDatabase()
    {
        return createDatabase;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getDropDatabase()
    {
        return dropDatabase;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getRenameDatabase()
    {
        return renameDatabase;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getCreateTable()
    {
        return createTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getDropTable()
    {
        return dropTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getReplaceTable()
    {
        return replaceTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetPartitionNames()
    {
        return getPartitionNames;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetPartitions()
    {
        return getPartitions;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetPartition()
    {
        return getPartition;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetPartitionByName()
    {
        return getPartitionByName;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getAddPartitions()
    {
        return addPartitions;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getDropPartition()
    {
        return dropPartition;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getAlterPartition()
    {
        return alterPartition;
    }
}
