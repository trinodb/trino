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

import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.session.PropertyMetadata;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.MetadataListing.listCatalogs;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

abstract class AbstractPropertiesSystemTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final Function<CatalogHandle, Collection<PropertyMetadata<?>>> catalogProperties;

    protected AbstractPropertiesSystemTable(String tableName, Metadata metadata, AccessControl accessControl, Function<CatalogHandle, Collection<PropertyMetadata<?>>> catalogProperties)
    {
        this.tableMetadata = tableMetadataBuilder(new SchemaTableName("metadata", tableName))
                .column("catalog_name", createUnboundedVarcharType())
                .column("property_name", createUnboundedVarcharType())
                .column("default_value", createUnboundedVarcharType())
                .column("type", createUnboundedVarcharType())
                .column("description", createUnboundedVarcharType())
                .build();
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null");
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
    public final RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(tableMetadata);

        List<CatalogInfo> catalogInfos = listCatalogs(session, metadata, accessControl).stream()
                .sorted(Comparator.comparing(CatalogInfo::getCatalogName))
                .collect(toImmutableList());

        for (CatalogInfo catalogInfo : catalogInfos) {
            catalogProperties.apply(catalogInfo.getCatalogHandle()).stream()
                    .sorted(Comparator.comparing(PropertyMetadata::getName))
                    .forEach(propertyMetadata ->
                            table.addRow(
                                    catalogInfo.getCatalogName(),
                                    propertyMetadata.getName(),
                                    firstNonNull(propertyMetadata.getDefaultValue(), "").toString(),
                                    propertyMetadata.getSqlType().toString(),
                                    propertyMetadata.getDescription()));
        }
        return table.build().cursor();
    }
}
