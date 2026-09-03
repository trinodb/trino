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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.hive.formats.avro.AvroTypeBlockHandler;
import io.trino.hive.formats.avro.AvroTypeException;
import io.trino.hive.formats.avro.HiveAvroTypeBlockHandler;
import io.trino.metastore.Column;
import io.trino.metastore.Table;
import io.trino.plugin.hive.avro.AvroHiveFileUtils;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;

/**
 * For Avro tables backed by {@code avro.schema.url}/{@code avro.schema.literal}, the Avro schema is the
 * authoritative definition of the table's columns, not the column list stored in the catalog. The Thrift
 * metastore client resolves it this way via the metastore's {@code get_fields} RPC (see
 * {@code BridgingHiveMetastore#getTable}); this class provides the equivalent for the Glue metastore client,
 * whose stored columns can drift from the Avro schema and then fail at split-read time.
 */
final class GlueAvroSchemaResolver
{
    // Shared: typeFor does not read file metadata, so configure() is never called and no per-file state is held.
    // The timestamp precision is arbitrary here, as HiveTypeTranslator maps every TimestampType to Hive "timestamp".
    private static final AvroTypeBlockHandler TYPE_HANDLER = new HiveAvroTypeBlockHandler(TIMESTAMP_MILLIS);

    private GlueAvroSchemaResolver() {}

    public static Table withColumnsFromAvroSchema(TrinoFileSystem fileSystem, Table table)
            throws IOException, AvroTypeException
    {
        Schema schema = AvroHiveFileUtils.determineSchemaOrThrowException(fileSystem, getHiveSchema(table));
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IOException("Avro schema for table is not a record: " + schema.getType());
        }

        // The Avro schema may also list the partition columns, which are stored separately as partition keys.
        Set<String> partitionColumnNames = table.getPartitionColumns().stream()
                .map(column -> column.getName().toLowerCase(Locale.ENGLISH))
                .collect(toImmutableSet());

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (Schema.Field field : schema.getFields()) {
            // Hive's Avro SerDe lower cases field names when producing column names
            String name = field.name().toLowerCase(Locale.ENGLISH);
            if (partitionColumnNames.contains(name)) {
                continue;
            }
            columns.add(new Column(name, toHiveType(TYPE_HANDLER.typeFor(field.schema())), Optional.ofNullable(field.doc()), ImmutableMap.of()));
        }

        List<Column> dataColumns = columns.build();
        if (dataColumns.isEmpty()) {
            throw new IOException("Avro schema for table declares no data columns");
        }
        return Table.builder(table)
                .setDataColumns(dataColumns)
                .build();
    }
}
