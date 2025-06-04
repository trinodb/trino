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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.iceberg.procedure.MigrationUtils.RecursiveDirectory;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeManager;

import java.util.Map;

import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ADD_FILES_FROM_TABLE;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class AddFilesTableFromTableProcedure
        implements Provider<TableProcedureMetadata>
{
    private final TypeManager typeManager;

    @Inject
    public AddFilesTableFromTableProcedure(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                ADD_FILES_FROM_TABLE.name(),
                coordinatorOnly(),
                ImmutableList.<PropertyMetadata<?>>builder()
                        .add(stringProperty(
                                "schema_name",
                                "Source schema name",
                                null,
                                false))
                        .add(stringProperty(
                                "table_name",
                                "Source table name",
                                null,
                                false))
                        .add(new PropertyMetadata<>(
                                "partition_filter",
                                "Partition filter",
                                new MapType(VARCHAR, VARCHAR, typeManager.getTypeOperators()),
                                Map.class,
                                null,
                                false,
                                object -> (Map<?, ?>) object,
                                Object::toString))
                        .add(enumProperty(
                                "recursive_directory",
                                "Recursive directory",
                                RecursiveDirectory.class,
                                RecursiveDirectory.FAIL,
                                false))
                        .build());
    }
}
