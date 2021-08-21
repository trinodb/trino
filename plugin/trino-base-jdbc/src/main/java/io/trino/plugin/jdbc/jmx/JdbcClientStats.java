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
package io.trino.plugin.jdbc.jmx;

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public final class JdbcClientStats
{
    private final JdbcApiStats abortReadConnection = new JdbcApiStats();
    private final JdbcApiStats addColumn = new JdbcApiStats();
    private final JdbcApiStats beginCreateTable = new JdbcApiStats();
    private final JdbcApiStats beginInsertTable = new JdbcApiStats();
    private final JdbcApiStats buildInsertSql = new JdbcApiStats();
    private final JdbcApiStats prepareQuery = new JdbcApiStats();
    private final JdbcApiStats buildSql = new JdbcApiStats();
    private final JdbcApiStats implementJoin = new JdbcApiStats();
    private final JdbcApiStats commitCreateTable = new JdbcApiStats();
    private final JdbcApiStats createSchema = new JdbcApiStats();
    private final JdbcApiStats createTable = new JdbcApiStats();
    private final JdbcApiStats setColumnComment = new JdbcApiStats();
    private final JdbcApiStats dropColumn = new JdbcApiStats();
    private final JdbcApiStats dropSchema = new JdbcApiStats();
    private final JdbcApiStats renameSchema = new JdbcApiStats();
    private final JdbcApiStats dropTable = new JdbcApiStats();
    private final JdbcApiStats finishInsertTable = new JdbcApiStats();
    private final JdbcApiStats getColumns = new JdbcApiStats();
    private final JdbcApiStats getConnectionWithHandle = new JdbcApiStats();
    private final JdbcApiStats getConnectionWithSplit = new JdbcApiStats();
    private final JdbcApiStats getPreparedStatement = new JdbcApiStats();
    private final JdbcApiStats getSchemaNames = new JdbcApiStats();
    private final JdbcApiStats getSplits = new JdbcApiStats();
    private final JdbcApiStats getTableHandle = new JdbcApiStats();
    private final JdbcApiStats getTableNames = new JdbcApiStats();
    private final JdbcApiStats getTableStatistics = new JdbcApiStats();
    private final JdbcApiStats renameColumn = new JdbcApiStats();
    private final JdbcApiStats renameTable = new JdbcApiStats();
    private final JdbcApiStats setTableProperties = new JdbcApiStats();
    private final JdbcApiStats rollbackCreateTable = new JdbcApiStats();
    private final JdbcApiStats schemaExists = new JdbcApiStats();
    private final JdbcApiStats toPrestoType = new JdbcApiStats();
    private final JdbcApiStats getColumnMappings = new JdbcApiStats();
    private final JdbcApiStats toWriteMapping = new JdbcApiStats();
    private final JdbcApiStats implementAggregation = new JdbcApiStats();
    private final JdbcApiStats getTableScanRedirection = new JdbcApiStats();
    private final JdbcApiStats delete = new JdbcApiStats();
    private final JdbcApiStats truncateTable = new JdbcApiStats();

    @Managed
    @Nested
    public JdbcApiStats getAbortReadConnection()
    {
        return abortReadConnection;
    }

    @Managed
    @Nested
    public JdbcApiStats getAddColumn()
    {
        return addColumn;
    }

    @Managed
    @Nested
    public JdbcApiStats getBeginCreateTable()
    {
        return beginCreateTable;
    }

    @Managed
    @Nested
    public JdbcApiStats getBeginInsertTable()
    {
        return beginInsertTable;
    }

    @Managed
    @Nested
    public JdbcApiStats getBuildInsertSql()
    {
        return buildInsertSql;
    }

    @Managed
    @Nested
    public JdbcApiStats getPrepareQuery()
    {
        return prepareQuery;
    }

    @Managed
    @Nested
    public JdbcApiStats getBuildSql()
    {
        return buildSql;
    }

    @Managed
    @Nested
    public JdbcApiStats getImplementJoin()
    {
        return implementJoin;
    }

    @Managed
    @Nested
    public JdbcApiStats getCommitCreateTable()
    {
        return commitCreateTable;
    }

    @Managed
    @Nested
    public JdbcApiStats getCreateSchema()
    {
        return createSchema;
    }

    @Managed
    @Nested
    public JdbcApiStats getCreateTable()
    {
        return createTable;
    }

    @Managed
    @Nested
    public JdbcApiStats getSetColumnComment()
    {
        return setColumnComment;
    }

    @Managed
    @Nested
    public JdbcApiStats getDropColumn()
    {
        return dropColumn;
    }

    @Managed
    @Nested
    public JdbcApiStats getDropSchema()
    {
        return dropSchema;
    }

    @Managed
    @Nested
    public JdbcApiStats getRenameSchema()
    {
        return renameSchema;
    }

    @Managed
    @Nested
    public JdbcApiStats getDropTable()
    {
        return dropTable;
    }

    @Managed
    @Nested
    public JdbcApiStats getFinishInsertTable()
    {
        return finishInsertTable;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetColumns()
    {
        return getColumns;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetConnectionWithHandle()
    {
        return getConnectionWithHandle;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetConnectionWithSplit()
    {
        return getConnectionWithSplit;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetPreparedStatement()
    {
        return getPreparedStatement;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetSchemaNames()
    {
        return getSchemaNames;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetSplits()
    {
        return getSplits;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetTableHandle()
    {
        return getTableHandle;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetTableNames()
    {
        return getTableNames;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetTableStatistics()
    {
        return getTableStatistics;
    }

    @Managed
    @Nested
    public JdbcApiStats getRenameColumn()
    {
        return renameColumn;
    }

    @Managed
    @Nested
    public JdbcApiStats getRenameTable()
    {
        return renameTable;
    }

    @Managed
    @Nested
    public JdbcApiStats getSetTableProperties()
    {
        return setTableProperties;
    }

    @Managed
    @Nested
    public JdbcApiStats getRollbackCreateTable()
    {
        return rollbackCreateTable;
    }

    @Managed
    @Nested
    public JdbcApiStats getSchemaExists()
    {
        return schemaExists;
    }

    @Managed
    @Nested
    public JdbcApiStats getToPrestoType()
    {
        return toPrestoType;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetColumnMappings()
    {
        return getColumnMappings;
    }

    @Managed
    @Nested
    public JdbcApiStats getToWriteMapping()
    {
        return toWriteMapping;
    }

    @Managed
    @Nested
    public JdbcApiStats getImplementAggregation()
    {
        return implementAggregation;
    }

    @Managed
    @Nested
    public JdbcApiStats getGetTableScanRedirection()
    {
        return getTableScanRedirection;
    }

    @Managed
    @Nested
    public JdbcApiStats getDelete()
    {
        return delete;
    }

    @Managed
    @Nested
    public JdbcApiStats getTruncateTable()
    {
        return truncateTable;
    }
}
