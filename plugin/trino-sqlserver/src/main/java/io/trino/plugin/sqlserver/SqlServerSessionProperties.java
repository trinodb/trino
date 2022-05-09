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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class SqlServerSessionProperties
        implements SessionPropertiesProvider
{
    public static final String BULK_COPY_FOR_WRITE = "bulk_copy_for_write";
    public static final String BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE = "bulk_copy_for_write_lock_destination_table";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public SqlServerSessionProperties(SqlServerConfig config)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        BULK_COPY_FOR_WRITE,
                        "Use SQL Server Bulk Copy API for writes",
                        config.isBulkCopyForWrite(),
                        false),
                booleanProperty(
                        BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE,
                        "Obtain a Bulk Update lock on destination table on write",
                        config.isBulkCopyForWriteLockDestinationTable(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    /**
     * Note that certain data types are unsupported by SQL Server and prevent
     * bulk copies from being performed, see the <a href="https://docs.microsoft.com/en-us/sql/connect/jdbc/use-bulk-copy-api-batch-insert-operation?view=sql-server-ver15#known-limitations">
     * SQL Server docs</a>.
     */
    public static boolean isBulkCopyForWrite(ConnectorSession session)
    {
        return session.getProperty(BULK_COPY_FOR_WRITE, Boolean.class);
    }

    public static boolean isBulkCopyForWriteLockDestinationTable(ConnectorSession session)
    {
        return session.getProperty(BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE, Boolean.class);
    }
}
