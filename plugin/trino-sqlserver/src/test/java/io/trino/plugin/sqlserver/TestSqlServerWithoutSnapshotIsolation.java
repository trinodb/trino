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

import io.trino.testing.sql.SqlExecutor;

import static java.lang.String.format;

public class TestSqlServerWithoutSnapshotIsolation
        extends BaseSqlServerTransactionIsolationTest
{
    @Override
    protected void configureDatabase(SqlExecutor executor, String databaseName)
    {
        // ALLOW_SNAPSHOT_ISOLATION controls whether SNAPSHOT ISOLATION is actually enabled
        executor.execute(format("ALTER DATABASE %s SET ALLOW_SNAPSHOT_ISOLATION OFF", databaseName));

        // READ_COMMITTED_SNAPSHOT that READ COMMITTED transaction isolation uses SNAPSHOT ISOLATION by default
        // it has no effect when ALLOW_SNAPSHOT_ISOLATION is disabled
        // https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server#snapshot-isolation-level-extensions
        executor.execute(format("ALTER DATABASE %s SET READ_COMMITTED_SNAPSHOT ON", databaseName));
    }
}
