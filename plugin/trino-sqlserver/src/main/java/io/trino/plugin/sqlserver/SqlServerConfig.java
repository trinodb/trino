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
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class SqlServerConfig
{
    private boolean snapshotIsolationDisabled;
    private boolean bulkCopyForWrite;
    private boolean bulkCopyForWriteLockDestinationTable;
    private boolean storedProcedureTableFunctionEnabled;
    private Duration connectSocketTimeout = new Duration(30, SECONDS);
    private Duration socketTimeout = new Duration(0, SECONDS);
    private int connectRetryCount;

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

    @Config("sqlserver.stored-procedure-table-function-enabled")
    @LegacyConfig("sqlserver.experimental.stored-procedure-table-function-enabled")
    @ConfigDescription("Allows accessing Stored procedure as a table function")
    public SqlServerConfig setStoredProcedureTableFunctionEnabled(boolean storedProcedureTableFunctionEnabled)
    {
        this.storedProcedureTableFunctionEnabled = storedProcedureTableFunctionEnabled;
        return this;
    }

    @NotNull
    @MinDuration("0s")
    public Duration getConnectSocketTimeout()
    {
        return connectSocketTimeout;
    }

    @Config("sqlserver.connect-socket-timeout")
    @ConfigDescription("Maximum time a socket read can block while establishing a connection, so that connections to an unresponsive server fail instead of hanging indefinitely; 0 means no timeout")
    public SqlServerConfig setConnectSocketTimeout(Duration connectSocketTimeout)
    {
        this.connectSocketTimeout = connectSocketTimeout;
        return this;
    }

    @NotNull
    @MinDuration("0s")
    public Duration getSocketTimeout()
    {
        return socketTimeout;
    }

    @Config("sqlserver.socket-timeout")
    @ConfigDescription("Maximum time a socket read can block after the connection is established; 0 means no timeout")
    public SqlServerConfig setSocketTimeout(Duration socketTimeout)
    {
        this.socketTimeout = socketTimeout;
        return this;
    }

    @Min(0)
    @Max(255)
    public int getConnectRetryCount()
    {
        return connectRetryCount;
    }

    @Config("sqlserver.connect-retry-count")
    @ConfigDescription("Number of times the driver silently retries a broken connection, both when connecting and through idle connection resiliency; " +
            "0 disables retries, so that connection failures surface to Trino immediately and a transparent reconnect cannot silently reset the socket timeout")
    public SqlServerConfig setConnectRetryCount(int connectRetryCount)
    {
        this.connectRetryCount = connectRetryCount;
        return this;
    }
}
