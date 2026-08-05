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
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_LITERAL_KEY;
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_URL_KEY;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;

/**
 * For Avro tables backed by {@code avro.schema.url}/{@code avro.schema.literal}, the Avro schema is
 * the authoritative definition of the table's columns, not the column list stored in the catalog.
 * <p>
 * The Thrift metastore client already resolves the effective schema this way: {@code
 * BridgingHiveMetastore#getTable} detects such tables and calls the metastore's {@code get_fields}
 * RPC, which resolves the live schema from the Avro file, instead of trusting the stored columns
 * from {@code get_table}. The Glue metastore client has no equivalent, so tables whose stored
 * columns have drifted from their Avro schema fail at split-read time in {@code
 * AvroPageSourceFactory} rather than being resolved transparently. Drift is expected for tables
 * mirrored into Glue by HMS event listeners, which copy the stored columns verbatim with no
 * Avro-schema awareness.
 * <p>
 * This class closes that gap for the Glue metastore client: when a table is Avro with a schema set,
 * its data columns are derived from the Avro schema rather than trusted from Glue's stored
 * {@code StorageDescriptor}.
 */
final class GlueAvroSchemaResolver
{
    // Only used for typeFor, which does not depend on file metadata, so configure() is never needed
    // and the instance carries no per-file state. The timestamp precision is irrelevant here:
    // HiveTypeTranslator maps every TimestampType to the precision-less Hive "timestamp".
    private static final AvroTypeBlockHandler TYPE_HANDLER = new HiveAvroTypeBlockHandler(TIMESTAMP_MILLIS);

    private GlueAvroSchemaResolver() {}

    public static boolean isAvroTableWithSchemaSet(Table table)
    {
        if (!AVRO.getSerde().equals(table.getStorage().getStorageFormat().getSerDeNullable())) {
            return false;
        }
        return table.getParameters().get(AVRO_SCHEMA_URL_KEY) != null ||
                table.getStorage().getSerdeParameters().get(AVRO_SCHEMA_URL_KEY) != null ||
                table.getParameters().get(AVRO_SCHEMA_LITERAL_KEY) != null ||
                table.getStorage().getSerdeParameters().get(AVRO_SCHEMA_LITERAL_KEY) != null;
    }

    public static Table withColumnsFromAvroSchema(TrinoFileSystem fileSystem, Table table)
            throws IOException, AvroTypeException
    {
        // getHiveSchema merges the serde parameters and the table parameters, matching the property
        // map the read path resolves against in AvroPageSourceFactory. Reading only the serde
        // parameters would miss avro.schema.url and avro.schema.literal set as table properties,
        // which is where they conventionally live.
        Schema schema = AvroHiveFileUtils.determineSchemaOrThrowException(fileSystem, getHiveSchema(table));
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IOException("Avro schema for table is not a record: " + schema.getType());
        }

        // Partition columns are stored separately from data columns and are not part of the Avro
        // schema of the data files. The Thrift path gets this for free because get_fields excludes
        // partition keys; here they have to be filtered out explicitly, otherwise a partition column
        // that does appear in the Avro schema would be reported twice.
        Set<String> partitionColumnNames = table.getPartitionColumns().stream()
                .map(column -> column.getName().toLowerCase(Locale.ENGLISH))
                .collect(toImmutableSet());

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (Schema.Field field : schema.getFields()) {
            // Hive column names are lower case, and the Thrift path returns lower case names because
            // Hive's Avro SerDe lower cases them when producing field schemas
            String name = field.name().toLowerCase(Locale.ENGLISH);
            if (partitionColumnNames.contains(name)) {
                continue;
            }
            // Reuse the type mapping the Avro read path itself uses, so the columns reported to the
            // planner cannot disagree with the types AvroPageSource produces for the same schema
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
