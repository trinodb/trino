/**
 * Licensed to the Airtel International LLP (AILLP) under one
 * or more contributor license agreements.
 * The AILLP licenses this file to you under the AA License, Version 1.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Vishal Donderia
 * @department Big Data Analytics Airtel Africa
 * @since Thu, 17-02-2022
 */
package io.trino.plugin.hive.procedure;

import com.airtel.africa.canvas.schemaGenerator.utils.SchemaLoader;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveMetastoreClosure;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.StylusMetadataConfig;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;

import javax.inject.Inject;
import javax.inject.Provider;
import java.lang.invoke.MethodHandle;
import java.sql.SQLException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class UpdateHiveSchemaProcedure
        implements Provider<Procedure>
{

    private final String ORC_SCHEMA_SEPARATOR = ",";
    private final String ORC_COLUMN_NAME_TYPE_SEPARATOR = ":";
    private static final MethodHandle GET_HIVE_SCHEMA = methodHandle(
            UpdateHiveSchemaProcedure.class,
            "updateHiveSchema",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class,
            String.class);

    private final TransactionalMetadataFactory hiveMetadataFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final HiveMetastoreClosure metastore;

    @Inject
    public UpdateHiveSchemaProcedure(HiveConfig hiveConfig, TransactionalMetadataFactory hiveMetadataFactory, HiveMetastore metastore, HdfsEnvironment hdfsEnvironment)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.metastore = new HiveMetastoreClosure(requireNonNull(metastore, "metastore is null"));
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "update_hive_schema",
                ImmutableList.of(
                        new Procedure.Argument("schema_name", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("dataset_name", VARCHAR),
                        new Procedure.Argument("dataset_store", VARCHAR)),
                GET_HIVE_SCHEMA.bindTo(this));
    }

    public void updateHiveSchema(ConnectorSession session, String schemaName, String tableName, String datasetName, String datasetStore) throws SQLException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            initializeMetadataConfig(session);
            String hiveOrcSchema = extractHiveSchema(session, schemaName, tableName);
            SchemaLoader.updateSchema(datasetName, hiveOrcSchema, datasetStore, StylusMetadataConfig.getStylusMetadataStoreConnectionDetails());
        }
    }

    private String extractHiveSchema(ConnectorSession session, String schemaName, String tableName)
    {

        HiveIdentity identity = new HiveIdentity(session);
        HdfsContext hdfsContext = new HdfsContext(session);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        Table table = metastore.getTable(identity, schemaName, tableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));

        // Merging table columns & partition columns
        return Stream.concat(table.getDataColumns().stream(), table.getPartitionColumns().stream())
                     .map(this::getOrcFormat)
                     .collect(Collectors.joining(ORC_SCHEMA_SEPARATOR));

    }

    private String getOrcFormat(Column c) {
        return c.getName() + ORC_COLUMN_NAME_TYPE_SEPARATOR + c.getType().getTypeInfo().getTypeName();
    }

    public void initializeMetadataConfig(ConnectorSession session)
    {
        Optional<String> configLocation = HiveSessionProperties.getStylusMetadataConfigLocation(session);
        if (configLocation.isEmpty()) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "metadata config location not specified in hive catalog");
        }
        StylusMetadataConfig.initializeStylusMetadataConfig(hdfsEnvironment, session, configLocation.get());
    }
}
