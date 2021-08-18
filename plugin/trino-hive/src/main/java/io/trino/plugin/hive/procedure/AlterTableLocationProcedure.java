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
package io.trino.plugin.hive.procedure;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;

import static io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import static io.trino.plugin.hive.util.HiveWriteUtils.isS3FileSystem;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;

public class AlterTableLocationProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle ALTER_TABLE_LOCATION = methodHandle(
            AlterTableLocationProcedure.class,
            "alterTableLocation",
            ConnectorSession.class,
            ConnectorAccessControl.class,
            String.class,
            String.class,
            String.class);

    private final TransactionalMetadataFactory hiveMetadataFactory;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public AlterTableLocationProcedure(
            TransactionalMetadataFactory hiveMetadataFactory,
            HdfsEnvironment hdfsEnvironment)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "alter_table_location",
                ImmutableList.of(
                        new Argument("schema_name", VARCHAR),
                        new Argument("table_name", VARCHAR),
                        new Argument("location", VARCHAR)),
                ALTER_TABLE_LOCATION.bindTo(this));
    }

    public void alterTableLocation(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, String location)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doAlterTableLocation(session, accessControl, schemaName, tableName, location);
        }
    }

    private void doAlterTableLocation(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, String location)
    {
        HdfsContext hdfsContext = new HdfsContext(session);
        HiveIdentity identity = new HiveIdentity(session);
        SemiTransactionalHiveMetastore metastore = hiveMetadataFactory.create().getMetastore();
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        Table table = metastore.getTable(identity, schemaName, tableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));

        accessControl.checkCanInsertIntoTable(null, schemaTableName);

        if (!table.getTableType().equals(EXTERNAL_TABLE.name())) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid table type: " + table.getTableType());
        }

        checkExternalPath(hdfsContext, new Path(location));

        metastore.alterTableLocation(identity, schemaName, tableName, location);
        metastore.commit();
    }

    private void checkExternalPath(HdfsContext context, Path path)
    {
        try {
            if (!isS3FileSystem(context, hdfsEnvironment, path)) {
                if (!hdfsEnvironment.getFileSystem(context, path).getFileStatus(path).isDirectory()) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, "External location must be a directory: " + path);
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "External location is not a valid file system URI: " + path, e);
        }
    }
}
