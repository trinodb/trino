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
package io.trino.plugin.hsqldb;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.immutableEnumSet;
import static io.trino.spi.connector.ConnectorCapabilities.DEFAULT_COLUMN_VALUE;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static java.util.Objects.requireNonNull;

public class HsqlDbConnector
        extends JdbcConnector
{
    private final List<PropertyMetadata<?>> columnProperties;

    @Inject
    public HsqlDbConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorSplitManager jdbcSplitManager,
            ConnectorPageSourceProvider jdbcPageSourceProvider,
            ConnectorPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionProperties,
            Set<TablePropertiesProvider> tableProperties,
            HsqlDbColumnProperties columnProperties,
            JdbcTransactionManager transactionManager)
    {
        super(lifeCycleManager, jdbcSplitManager, jdbcPageSourceProvider, jdbcPageSinkProvider, accessControl, procedures, connectorTableFunctions, sessionProperties, tableProperties, transactionManager);
        requireNonNull(columnProperties, "columnProperties is null");
        this.columnProperties = columnProperties.getColumnProperties();
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(
                DEFAULT_COLUMN_VALUE,
                NOT_NULL_COLUMN_CONSTRAINT);
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }
}
