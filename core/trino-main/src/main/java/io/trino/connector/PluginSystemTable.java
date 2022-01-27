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
package io.trino.connector;

import io.trino.metadata.Metadata;
import io.trino.server.PluginManager;
import io.trino.server.PluginMetadata;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.List;

import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class PluginSystemTable
        implements SystemTable
{
    public static final SchemaTableName TABLE_NAME = new SchemaTableName("metadata", "plugins");

    public static final ConnectorTableMetadata TABLE = tableMetadataBuilder(TABLE_NAME)
            .column("plugin_name", createUnboundedVarcharType())
            .column("connectors", new ArrayType(createUnboundedVarcharType()))
            .column("functions", new ArrayType(createUnboundedVarcharType()))
            .column("event_listeners", new ArrayType(createUnboundedVarcharType()))
            .build();
    private final PluginManager pluginManager;

    @Inject
    public PluginSystemTable(Metadata metadata, PluginManager pluginManager)
    {
        this.pluginManager = requireNonNull(pluginManager, "pluginManager is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(TABLE);
        for (PluginMetadata plugin : pluginManager.getPluginsMetadata()) {
            table.addRow(
                    plugin.getName(),
                    getStringsBlock(plugin.getConnectors()),
                    getStringsBlock(plugin.getFunctions()),
                    getStringsBlock(plugin.getEventListeners()));
        }
        return table.build().cursor();
    }

    private static Block getStringsBlock(List<String> input)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, input.size());
        input.forEach(name -> VARCHAR.writeString(builder, name));
        return builder.build();
    }
}
