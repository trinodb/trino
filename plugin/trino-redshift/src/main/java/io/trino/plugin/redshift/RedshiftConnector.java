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
package io.trino.plugin.redshift;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.FileSystemS3;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;

import java.util.Optional;
import java.util.Set;

public class RedshiftConnector
        extends JdbcConnector
{
    private final RedshiftPageSourceProvider pageSourceProvider;

    @Inject
    public RedshiftConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorSplitManager jdbcSplitManager,
            ConnectorRecordSetProvider jdbcRecordSetProvider,
            ConnectorPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionProperties,
            Set<TablePropertiesProvider> tableProperties,
            JdbcTransactionManager transactionManager,
            @FileSystemS3 TrinoFileSystemFactory fileSystemFactory)
    {
        super(
                lifeCycleManager,
                jdbcSplitManager,
                jdbcRecordSetProvider,
                jdbcPageSinkProvider,
                accessControl,
                procedures,
                connectorTableFunctions,
                sessionProperties,
                tableProperties,
                transactionManager);
        this.pageSourceProvider = new RedshiftPageSourceProvider(jdbcRecordSetProvider, fileSystemFactory);
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        // throwing this exception will ensure using the RedshiftPageSourceProvider, that supports two modes of operation
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }
}
