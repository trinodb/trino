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
package io.trino.metastore.lakehouse;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.biglake.hive.v1.CatalogName;
import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceClient;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceSettings;
import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.ListPartitionsRequest;
import com.google.cloud.biglake.hive.v1.ListPartitionsResponse;
import com.google.cloud.biglake.hive.v1.NamespaceName;
import com.google.cloud.biglake.hive.v1.Partition;
import com.google.cloud.biglake.hive.v1.TableName;
import com.google.common.collect.ImmutableList;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static io.trino.metastore.lakehouse.LakehouseErrorTranslator.isMissingParent;
import static io.trino.metastore.lakehouse.LakehouseErrorTranslator.isNotFound;
import static io.trino.metastore.lakehouse.LakehouseErrorTranslator.translate;
import static java.util.Objects.requireNonNull;

/**
 * Reads a Lakehouse Hive catalog over gRPC.
 */
public class GrpcLakehouseClient
        implements LakehouseClient, Closeable
{
    private final HiveMetastoreServiceClient client;
    private final String projectId;
    private final String catalogId;

    public GrpcLakehouseClient(HiveMetastoreServiceClient client, String projectId, String catalogId)
    {
        this.client = requireNonNull(client, "client is null");
        this.projectId = requireNonNull(projectId, "projectId is null");
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
    }

    /**
     * Returns the settings of the client library: a user agent, and the same timeout without
     * retries for every call, including the stream of partitions.
     */
    static HiveMetastoreServiceSettings.Builder serviceSettings(Duration requestTimeout)
    {
        HiveMetastoreServiceSettings.Builder settings = HiveMetastoreServiceSettings.newBuilder()
                .setHeaderProvider(FixedHeaderProvider.create("user-agent", "Trino"));
        settings.applyToAllUnaryMethods(builder -> {
            builder.setSimpleTimeoutNoRetriesDuration(requestTimeout);
            return null;
        });
        settings.listPartitionsSettings().setSimpleTimeoutNoRetriesDuration(requestTimeout);
        return settings;
    }

    @Override
    public List<HiveDatabase> listDatabases()
    {
        try {
            return ImmutableList.copyOf(
                    client.listHiveDatabases(CatalogName.of(projectId, catalogId)).iterateAll());
        }
        catch (ApiException e) {
            throw translate("listing databases", e);
        }
    }

    @Override
    public Optional<HiveDatabase> getDatabase(String databaseName)
    {
        try {
            return Optional.of(client.getHiveDatabase(namespace(databaseName)));
        }
        catch (ApiException e) {
            if (isNotFound(e)) {
                return Optional.empty();
            }
            throw translate("reading database " + databaseName, e);
        }
    }

    @Override
    public List<HiveTable> listTables(String databaseName)
    {
        try {
            return ImmutableList.copyOf(client.listHiveTables(namespace(databaseName)).iterateAll());
        }
        catch (ApiException e) {
            if (isNotFound(e) || isMissingParent(e)) {
                return ImmutableList.of();
            }
            throw translate("listing tables of " + databaseName, e);
        }
    }

    @Override
    public Optional<HiveTable> getTable(String databaseName, String tableName)
    {
        try {
            return Optional.of(client.getHiveTable(
                    TableName.of(projectId, catalogId, databaseName, tableName)));
        }
        catch (ApiException e) {
            if (isNotFound(e) || isMissingParent(e)) {
                return Optional.empty();
            }
            throw translate("reading table " + databaseName + "." + tableName, e);
        }
    }

    @Override
    public Optional<List<Partition>> listPartitions(String databaseName, String tableName, String filter)
    {
        ListPartitionsRequest request = ListPartitionsRequest.newBuilder()
                .setParent(TableName.format(projectId, catalogId, databaseName, tableName))
                .setFilter(filter)
                .build();
        try {
            ServerStream<ListPartitionsResponse> stream = client.listPartitionsCallable().call(request);
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
            for (ListPartitionsResponse response : stream) {
                partitions.addAll(response.getPartitionsList());
            }
            return Optional.of(partitions.build());
        }
        catch (ApiException e) {
            if (isNotFound(e) || isMissingParent(e)) {
                return Optional.empty();
            }
            throw translate("listing partitions of " + databaseName + "." + tableName, e);
        }
    }

    private NamespaceName namespace(String databaseName)
    {
        return NamespaceName.of(projectId, catalogId, databaseName);
    }

    @Override
    public void close()
    {
        client.close();
    }
}
