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
package io.trino.plugin.pulsar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.base.CatalogName;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTableLayoutResult;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaType;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_DATA_FORMAT;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_FORMAT_HINT;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_HANDLE_TYPE;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_INTERNAL;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_MAPPING;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_NAME_CASE_SENSITIVE;
import static io.trino.plugin.pulsar.PulsarConnectorUtils.restoreNamespaceDelimiterIfNeeded;
import static io.trino.plugin.pulsar.PulsarErrorCode.PULSAR_ADMIN_ERROR;
import static io.trino.plugin.pulsar.PulsarErrorCode.PULSAR_SCHEMA_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Metadata for Trino Pulsar connector.
 */
public class PulsarMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(PulsarMetadata.class);

    private static final String INFORMATION_SCHEMA = "information_schema";

    private final String catalogName;
    private final boolean hideInternalColumns;
    private final PulsarConnectorConfig pulsarConnectorConfig;

    private final PulsarDispatchingRowDecoderFactory decoderFactory;

    private final LoadingCache<SchemaTableName, TopicName> tableNameTopicNameCache =
            CacheBuilder.newBuilder()
                    // use a short live cache to make sure one query not get matched the topic many times and
                    // prevent get the wrong cache due to the topic changes in the Pulsar.
                    .expireAfterWrite(30, TimeUnit.SECONDS)
                    .build(new CacheLoader<SchemaTableName, TopicName>()
                    {
                        @Override
                        public TopicName load(SchemaTableName schemaTableName) throws Exception
                        {
                            return getMatchedPulsarTopic(schemaTableName);
                        }
                    });

    @Inject
    public PulsarMetadata(
            CatalogName catalogName,
            PulsarConnectorConfig pulsarConnectorConfig,
            PulsarDispatchingRowDecoderFactory decoderFactory)
    {
        this.decoderFactory = decoderFactory;
        this.catalogName = requireNonNull(catalogName, "catalogName is null").toString();
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        requireNonNull(this.pulsarConnectorConfig.getWebServiceUrl(), "web-service-url is null");
        requireNonNull(this.pulsarConnectorConfig.getZookeeperUri(), "zookeeper-uri is null");
        this.hideInternalColumns = pulsarConnectorConfig.isHideInternalColumns();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        List<String> trinoSchemas = new LinkedList<>();
        try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)) {
            List<String> tenants = pulsarAdmin.tenants().getTenants();
            for (String tenant : tenants) {
                trinoSchemas.addAll(pulsarAdmin.namespaces().getNamespaces(tenant).stream().map(namespace ->
                        PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(namespace, pulsarConnectorConfig)).collect(Collectors.toList()));
            }
        }
        catch (PulsarAdminException e) {
            if (e.getStatusCode() == 401) {
                throw new TrinoException(PULSAR_ADMIN_ERROR, "fail to get schemas from pulsar: Unauthorized");
            }
            throw new TrinoException(PULSAR_SCHEMA_ERROR, "fail to list pulsar topic schema", e);
        }
        catch (PulsarClientException e) {
            throw new TrinoException(PULSAR_ADMIN_ERROR, "fail to create pulsar admin client", e);
        }
        return trinoSchemas;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session,
                                               SchemaTableName tableName)
    {
        TopicName topicName = getMatchedTopicName(tableName);
        return topicName == null ? null : new PulsarTableHandle(
                catalogName,
                tableName.getSchemaName(),
                tableName.getTableName(),
                topicName.getLocalName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        PulsarTableHandle handle = PulsarHandleResolver.convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new PulsarTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ConnectorTableMetadata connectorTableMetadata;
        SchemaTableName schemaTableName = PulsarHandleResolver.convertTableHandle(table).toSchemaTableName();
        connectorTableMetadata = getTableMetadata(schemaTableName, !hideInternalColumns, false);
        if (connectorTableMetadata == null) {
            ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
            connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, builder.build());
        }
        return connectorTableMetadata;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();

        if (schemaName.isPresent()) {
            String schemaNameOrNull = schemaName.get();

            if (schemaNameOrNull.equals(INFORMATION_SCHEMA)) {
                // no-op for now but add pulsar connector specific system tables here
            }
            else {
                List<String> pulsarTopicList = null;
                try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)) {
                    pulsarTopicList = pulsarAdmin.topics()
                            .getList(restoreNamespaceDelimiterIfNeeded(schemaNameOrNull, pulsarConnectorConfig),
                                    TopicDomain.persistent);
                }
                catch (PulsarAdminException e) {
                    if (e.getStatusCode() == 404) {
                        log.warn("Schema " + schemaNameOrNull + " does not exsit");
                        return builder.build();
                    }
                    else if (e.getStatusCode() == 401) {
                        throw new TrinoException(PULSAR_ADMIN_ERROR, "fail to get tables in: Unauthorized", e);
                    }
                    throw new TrinoException(PULSAR_ADMIN_ERROR, "fail to get tables", e);
                }
                catch (PulsarClientException e) {
                    throw new TrinoException(PULSAR_ADMIN_ERROR, "fail to create pulsar admin client", e);
                }
                if (pulsarTopicList != null) {
                    pulsarTopicList.stream()
                            .map(topic -> TopicName.get(topic).getPartitionedTopicName())
                            .distinct()
                            .forEach(topic -> builder.add(new SchemaTableName(schemaNameOrNull,
                                    TopicName.get(topic).getLocalName())));
                }
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PulsarTableHandle pulsarTableHandle = PulsarHandleResolver.convertTableHandle(tableHandle);

        ConnectorTableMetadata tableMetaData = getTableMetadata(pulsarTableHandle.toSchemaTableName(), false, true);
        if (tableMetaData == null) {
            return new HashMap<>();
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        tableMetaData.getColumns().forEach(columnMetadata -> {
            PulsarColumnHandle pulsarColumnHandle = new PulsarColumnHandle(
                    catalogName,
                    (String) columnMetadata.getProperties().get(PROPERTY_KEY_NAME_CASE_SENSITIVE),
                    columnMetadata.getType(),
                    columnMetadata.isHidden(),
                    columnMetadata.getProperties().containsKey(PROPERTY_KEY_INTERNAL) ? (Boolean) columnMetadata.getProperties().get(PROPERTY_KEY_INTERNAL) : false,
                    (String) columnMetadata.getProperties().get(PROPERTY_KEY_MAPPING),
                    (String) columnMetadata.getProperties().get(PROPERTY_KEY_DATA_FORMAT),
                    (String) columnMetadata.getProperties().get(PROPERTY_KEY_FORMAT_HINT),
                    Optional.of((PulsarColumnHandle.HandleKeyValueType) columnMetadata.getProperties().get(PROPERTY_KEY_HANDLE_TYPE)));

            columnHandles.put(columnMetadata.getName(), pulsarColumnHandle);
        });

        if (!hideInternalColumns) {
            PulsarInternalColumn.getInternalFields().forEach(pulsarInternalColumn -> {
                PulsarColumnHandle pulsarColumnHandle = pulsarInternalColumn.getColumnHandle(catalogName, false);
                columnHandles.put(pulsarColumnHandle.getName(), pulsarColumnHandle);
            });
        }

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        PulsarHandleResolver.convertTableHandle(tableHandle);
        return PulsarHandleResolver.convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (!prefix.getTable().isPresent()) {
            tableNames = listTables(session, prefix.getSchema());
        }
        else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
        }

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata connectorTableMetadata = getTableMetadata(tableName, !hideInternalColumns, false);
            if (connectorTableMetadata != null) {
                columns.put(tableName, connectorTableMetadata.getColumns());
            }
        }

        return columns.build();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return true;
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName, boolean withInternalColumns, boolean withInternalProperties)
    {
        if (schemaTableName.getSchemaName().equals(INFORMATION_SCHEMA)) {
            return null;
        }

        TopicName topicName = getMatchedTopicName(schemaTableName);

        if (topicName == null) {
            return null;
        }

        SchemaInfo schemaInfo;
        try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)) {
            schemaInfo = pulsarAdmin.schemas().getSchemaInfo(topicName.getSchemaName());
        }
        catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                // use default schema because there is no schema
                schemaInfo = PulsarSchemaInfoProvider.defaultSchema();
            }
            else if (e.getStatusCode() == 401) {
                throw new TrinoException(PULSAR_ADMIN_ERROR, format("fail to get schema metadata for topic %s: Unauthorized", topicName), e);
            }
            else {
                throw new TrinoException(PULSAR_ADMIN_ERROR, "fail to get schema metadata for topic", e);
            }
        }
        catch (PulsarClientException e) {
            throw new TrinoException(PULSAR_ADMIN_ERROR, "fail to create pulsar admin client", e);
        }
        List<ColumnMetadata> handles = getPulsarColumns(topicName, schemaInfo, withInternalColumns, withInternalProperties, PulsarColumnHandle.HandleKeyValueType.NONE);

        return new ConnectorTableMetadata(schemaTableName, handles);
    }

    /**
     * Convert pulsar schema into trino table metadata.
     */
    @VisibleForTesting
    public List<ColumnMetadata> getPulsarColumns(
            TopicName topicName,
            SchemaInfo schemaInfo,
            boolean withInternalColumns,
            boolean withInternalProperties,
            PulsarColumnHandle.HandleKeyValueType handleKeyValueType)
    {
        SchemaType schemaType = schemaInfo.getType();
        if (schemaType.isStruct() || schemaType.isPrimitive()) {
            return getPulsarColumnsFromSchema(topicName, schemaInfo, withInternalColumns, withInternalProperties, handleKeyValueType);
        }
        else if (schemaType.equals(SchemaType.KEY_VALUE)) {
            return getPulsarColumnsFromKeyValueSchema(topicName, schemaInfo, withInternalProperties, withInternalColumns);
        }
        else {
            throw new IllegalArgumentException("Unsupported schema : " + schemaInfo);
        }
    }

    List<ColumnMetadata> getPulsarColumnsFromSchema(
            TopicName topicName,
            SchemaInfo schemaInfo,
            boolean withInternalColumns,
            boolean withInternalProperties,
            PulsarColumnHandle.HandleKeyValueType handleKeyValueType)
    {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        builder.addAll(decoderFactory.extractColumnMetadata(topicName, schemaInfo, handleKeyValueType, withInternalProperties));
        if (withInternalColumns) {
            PulsarInternalColumn.getInternalFields()
                    .stream().forEach(pulsarInternalColumn -> builder.add(pulsarInternalColumn.getColumnMetadata(false)));
        }
        return builder.build();
    }

    List<ColumnMetadata> getPulsarColumnsFromKeyValueSchema(
            TopicName topicName,
            SchemaInfo schemaInfo,
            boolean withInternalColumns,
            boolean withInternalProperties)
    {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
        SchemaInfo keySchemaInfo = kvSchemaInfo.getKey();
        List<ColumnMetadata> keyColumnMetadataList = getPulsarColumns(topicName, keySchemaInfo, withInternalColumns, withInternalProperties, PulsarColumnHandle.HandleKeyValueType.KEY);
        builder.addAll(keyColumnMetadataList);

        SchemaInfo valueSchemaInfo = kvSchemaInfo.getValue();
        List<ColumnMetadata> valueColumnMetadataList = getPulsarColumns(topicName, valueSchemaInfo, withInternalColumns, withInternalProperties, PulsarColumnHandle.HandleKeyValueType.VALUE);
        builder.addAll(valueColumnMetadataList);

        if (withInternalColumns) {
            PulsarInternalColumn.getInternalFields()
                    .forEach(pulsarInternalColumn -> builder.add(pulsarInternalColumn.getColumnMetadata(false)));
        }
        return builder.build();
    }

    private TopicName getMatchedTopicName(SchemaTableName schemaTableName)
    {
        TopicName topicName;
        try {
            topicName = tableNameTopicNameCache.get(schemaTableName);
        }
        catch (Exception e) {
            if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            return null;
        }
        return topicName;
    }

    private TopicName getMatchedPulsarTopic(SchemaTableName schemaTableName)
    {
        String namespace = restoreNamespaceDelimiterIfNeeded(schemaTableName.getSchemaName(), pulsarConnectorConfig);

        Set<String> topicsSetWithoutPartition = null;
        try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)) {
            List<String> allTopics = pulsarAdmin.topics().getList(namespace, TopicDomain.persistent);
            topicsSetWithoutPartition = allTopics.stream()
                    .map(t -> t.split(TopicName.PARTITIONED_TOPIC_SUFFIX)[0])
                    .collect(Collectors.toSet());
        }
        catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                throw new TrinoException(NOT_FOUND, "Schema " + namespace + " does not exist", e);
            }
            else if (e.getStatusCode() == 401) {
                throw new TrinoException(PULSAR_ADMIN_ERROR, format("fail to get topics in schema %s: Unauthorized", namespace), e);
            }
            throw new TrinoException(PULSAR_ADMIN_ERROR, format("fail to get topics in schema %s", namespace), e);
        }
        catch (PulsarClientException e) {
            throw new TrinoException(PULSAR_ADMIN_ERROR, "fail to create pulsar admin client", e);
        }

        List<String> matchedTopics = topicsSetWithoutPartition.stream()
                .filter(t -> TopicName.get(t).getLocalName().equalsIgnoreCase(schemaTableName.getTableName()))
                .collect(Collectors.toList());

        if (matchedTopics.size() == 0) {
            return null;
        }
        else if (matchedTopics.size() != 1) {
            String errMsg = format("There are multiple topics %s matched the table name %s",
                    matchedTopics.toString(), format("%s/%s", namespace, schemaTableName.getTableName()));
            throw new TableNotFoundException(schemaTableName, errMsg);
        }
        if (log.isDebugEnabled()) {
            log.debug("matched topic %s for table %s ", matchedTopics.get(0), schemaTableName);
        }
        return TopicName.get(matchedTopics.get(0));
    }
}
