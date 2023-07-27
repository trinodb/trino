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
package io.trino.plugin.hive.metastore;

import com.linkedin.coral.common.HiveMetastoreClient;
import io.trino.hive.thrift.metastore.Database;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.SerDeInfo;
import io.trino.hive.thrift.metastore.StorageDescriptor;
import io.trino.hive.thrift.metastore.Table;
import io.trino.plugin.hive.CoralTableRedirectionResolver;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiDatabase;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiTable;
import static java.util.Objects.requireNonNull;

/**
 * Class to adapt Trino's {@link SemiTransactionalHiveMetastore} to Coral's
 * {@link HiveMetastoreClient}. This allows reuse of the hive metastore instantiated by
 * Trino, based on configuration, inside Coral.
 */
public class CoralSemiTransactionalHiveMSCAdapter
        implements HiveMetastoreClient
{
    private final SemiTransactionalHiveMetastore delegate;
    private final CoralTableRedirectionResolver tableRedirection;

    public CoralSemiTransactionalHiveMSCAdapter(
            SemiTransactionalHiveMetastore coralHiveMetastoreClient,
            CoralTableRedirectionResolver tableRedirection)
    {
        this.delegate = requireNonNull(coralHiveMetastoreClient, "coralHiveMetastoreClient is null");
        this.tableRedirection = requireNonNull(tableRedirection, "tableRedirection is null");
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    // returning null for missing entry is as per Coral's requirements
    @Override
    public com.linkedin.coral.hive.metastore.api.Database getDatabase(String dbName)
    {
        return delegate.getDatabase(dbName)
                .map(database -> toHiveDatabase(toMetastoreApiDatabase(database)))
                .orElse(null);
    }

    @Override
    public List<String> getAllTables(String dbName)
    {
        return delegate.getAllTables(dbName);
    }

    @Override
    public com.linkedin.coral.hive.metastore.api.Table getTable(String dbName, String tableName)
    {
        if (!dbName.isEmpty() && !tableName.isEmpty()) {
            Optional<Table> redirected = tableRedirection.redirect(new SchemaTableName(dbName, tableName));
            if (redirected.isPresent()) {
                return toHiveTable(redirected.get());
            }
        }

        return delegate.getTable(dbName, tableName)
                .map(value -> toHiveTable(toMetastoreApiTable(value, NO_PRIVILEGES)))
                .orElse(null);
    }

    private static com.linkedin.coral.hive.metastore.api.Database toHiveDatabase(Database database)
    {
        var result = new com.linkedin.coral.hive.metastore.api.Database();
        result.setName(database.getName());
        result.setDescription(database.getDescription());
        result.setLocationUri(database.getLocationUri());
        result.setParameters(database.getParameters());
        return result;
    }

    private static com.linkedin.coral.hive.metastore.api.Table toHiveTable(Table table)
    {
        var result = new com.linkedin.coral.hive.metastore.api.Table();
        result.setDbName(table.getDbName());
        result.setTableName(table.getTableName());
        result.setTableType(table.getTableType());
        result.setViewOriginalText(table.getViewOriginalText());
        result.setViewExpandedText(table.getViewExpandedText());
        result.setPartitionKeys(table.getPartitionKeys().stream()
                .map(CoralSemiTransactionalHiveMSCAdapter::toHiveFieldSchema)
                .toList());
        result.setParameters(table.getParameters());
        result.setSd(toHiveStorageDescriptor(table.getSd()));
        return result;
    }

    private static com.linkedin.coral.hive.metastore.api.StorageDescriptor toHiveStorageDescriptor(StorageDescriptor storage)
    {
        var result = new com.linkedin.coral.hive.metastore.api.StorageDescriptor();
        result.setCols(storage.getCols().stream()
                .map(CoralSemiTransactionalHiveMSCAdapter::toHiveFieldSchema)
                .toList());
        result.setBucketCols(storage.getBucketCols());
        result.setNumBuckets(storage.getNumBuckets());
        result.setInputFormat(storage.getInputFormat());
        result.setOutputFormat(storage.getInputFormat());
        result.setSerdeInfo(toHiveSerdeInfo(storage.getSerdeInfo()));
        result.setLocation(storage.getLocation());
        result.setParameters(storage.getParameters());
        return result;
    }

    private static com.linkedin.coral.hive.metastore.api.SerDeInfo toHiveSerdeInfo(SerDeInfo info)
    {
        var result = new com.linkedin.coral.hive.metastore.api.SerDeInfo();
        result.setName(info.getName());
        result.setDescription(info.getDescription());
        result.setSerializationLib(info.getSerializationLib());
        result.setSerializerClass(info.getSerializerClass());
        result.setDeserializerClass(info.getDeserializerClass());
        result.setParameters(info.getParameters());
        return result;
    }

    private static com.linkedin.coral.hive.metastore.api.FieldSchema toHiveFieldSchema(FieldSchema field)
    {
        var result = new com.linkedin.coral.hive.metastore.api.FieldSchema();
        result.setName(field.getName());
        result.setType(field.getType());
        result.setComment(field.getComment());
        return result;
    }
}
