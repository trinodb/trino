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
package io.trino.plugin.sqlserver;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class SqlServerConfig
{
    private boolean snapshotIsolationDisabled;
    private boolean bulkCopyForWrite;
    private boolean bulkCopyForWriteLockDestinationTable;
    private boolean storedProcedureTableFunctionEnabled;

    public boolean isBulkCopyForWrite()
    {
        return bulkCopyForWrite;
    }

    @Config("sqlserver.bulk-copy-for-write.enabled")
    @ConfigDescription("Use SQL Server Bulk Copy API for writes")
    public SqlServerConfig setBulkCopyForWrite(boolean bulkCopyForWrite)
    {
        this.bulkCopyForWrite = bulkCopyForWrite;
        return this;
    }

    public boolean isBulkCopyForWriteLockDestinationTable()
    {
        return bulkCopyForWriteLockDestinationTable;
    }

    @Config("sqlserver.bulk-copy-for-write.lock-destination-table")
    @ConfigDescription("Obtain a Bulk Update lock on destination table on write")
    public SqlServerConfig setBulkCopyForWriteLockDestinationTable(boolean bulkCopyForWriteLockDestinationTable)
    {
        this.bulkCopyForWriteLockDestinationTable = bulkCopyForWriteLockDestinationTable;
        return this;
    }

    public boolean isSnapshotIsolationDisabled()
    {
        return snapshotIsolationDisabled;
    }

    @Config("sqlserver.snapshot-isolation.disabled")
    @ConfigDescription("Disables automatic use of snapshot isolation for transactions issued by Trino in SQL Server")
    public SqlServerConfig setSnapshotIsolationDisabled(boolean snapshotIsolationDisabled)
    {
        this.snapshotIsolationDisabled = snapshotIsolationDisabled;
        return this;
    }

    public boolean isStoredProcedureTableFunctionEnabled()
    {
        return storedProcedureTableFunctionEnabled;
    }

    @Config("sqlserver.experimental.stored-procedure-table-function-enabled")
    @ConfigDescription("Allows accessing Stored procedure as a table function")
    public SqlServerConfig setStoredProcedureTableFunctionEnabled(boolean storedProcedureTableFunctionEnabled)
    {
        this.storedProcedureTableFunctionEnabled = storedProcedureTableFunctionEnabled;
        return this;
    }
}
