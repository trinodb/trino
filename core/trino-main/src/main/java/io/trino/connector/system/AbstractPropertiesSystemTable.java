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
package io.trino.connector.system;

import io.trino.connector.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.PropertyProvider;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

abstract class AbstractPropertiesSystemTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final TransactionManager transactionManager;
    private final Supplier<Map<CatalogName, PropertyProvider>> propertySupplier;

    protected AbstractPropertiesSystemTable(String tableName, TransactionManager transactionManager, Supplier<Map<CatalogName, PropertyProvider>> propertySupplier)
    {
        this.tableMetadata = tableMetadataBuilder(new SchemaTableName("metadata", tableName))
                .column("catalog_name", createUnboundedVarcharType())
                .column("property_name", createUnboundedVarcharType())
                .column("default_value", createUnboundedVarcharType())
                .column("type", createUnboundedVarcharType())
                .column("description", createUnboundedVarcharType())
                .build();
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.propertySupplier = requireNonNull(propertySupplier, "propertySupplier is null");
    }

    @Override
    public final Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public final ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public final RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        TransactionId transactionId = ((GlobalSystemTransactionHandle) transactionHandle).getTransactionId();

        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(tableMetadata);
        Map<CatalogName, PropertyProvider> connectorProperties = propertySupplier.get();
        for (Entry<String, CatalogName> entry : new TreeMap<>(transactionManager.getCatalogNames(transactionId)).entrySet()) {
            PropertyProvider propertyProvider = connectorProperties.get(entry.getValue());
            propertyProvider.getKnownPropertyNames().stream()
                    .sorted()
                    .map(propertyProvider::getProperty)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(propertyMetadata -> table.addRow(
                            entry.getKey(),
                            propertyMetadata.getName(),
                            firstNonNull(propertyMetadata.getDefaultValue(), "").toString(),
                            propertyMetadata.getSqlType().toString(),
                            propertyMetadata.getDescription()));
        }
        return table.build().cursor();
    }
}
