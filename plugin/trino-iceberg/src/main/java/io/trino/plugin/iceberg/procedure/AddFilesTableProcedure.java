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
import com.google.inject.Provider;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.iceberg.procedure.MigrationUtils.RecursiveDirectory;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ADD_FILES;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class AddFilesTableProcedure
        implements Provider<TableProcedureMetadata>
{
    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                ADD_FILES.name(),
                coordinatorOnly(),
                ImmutableList.<PropertyMetadata<?>>builder()
                        .add(stringProperty(
                                "location",
                                "location",
                                null,
                                false))
                        .add(enumProperty(
                                "format",
                                "File format",
                                HiveStorageFormat.class,
                                null,
                                value -> checkProcedureArgument(value == ORC || value == PARQUET || value == AVRO, "The procedure does not support storage format: %s", value),
                                false))
                        .add(enumProperty(
                                "recursive_directory",
                                "Recursive directory",
                                RecursiveDirectory.class,
                                RecursiveDirectory.FAIL,
                                false))
                        .build());
    }
}
