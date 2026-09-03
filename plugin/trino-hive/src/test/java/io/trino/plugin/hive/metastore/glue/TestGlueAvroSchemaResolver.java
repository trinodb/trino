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
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.metastore.Column;
import io.trino.metastore.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_LITERAL_KEY;
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_URL_KEY;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.metastore.glue.GlueAvroSchemaResolver.isAvroTableWithSchemaSet;
import static io.trino.plugin.hive.metastore.glue.GlueAvroSchemaResolver.withColumnsFromAvroSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Glue-stored columns in these tables are deliberately wrong — a superset that does not match
 * the Avro schema — because that is the drift condition this resolver exists to correct.
 */
final class TestGlueAvroSchemaResolver
{
    private static final TrinoFileSystem FILE_SYSTEM = new LocalFileSystem(Path.of("/"));

    private static final String SCHEMA_LITERAL =
            """
            {
              "type": "record",
              "name": "Envelope",
              "namespace": "com.example.avro",
              "fields": [
                {"name": "event_id", "type": "string", "doc": "the id"},
                {"name": "amount", "type": ["null", "int"], "default": null}
              ]
            }
            """;

    // What Glue has stored: stale, and a superset of the Avro schema
    private static final List<Column> STORED_COLUMNS = ImmutableList.of(
            new Column("event_id", HIVE_STRING, Optional.empty(), ImmutableMap.of()),
            new Column("amount", HIVE_INT, Optional.empty(), ImmutableMap.of()),
            new Column("feedback_timestamp", HIVE_STRING, Optional.empty(), ImmutableMap.of()));

    @Test
    void testResolvesColumnsFromSchemaLiteralInTableParameters()
            throws Exception
    {
        assertResolvesEnvelope(avroTable(ImmutableMap.of(AVRO_SCHEMA_LITERAL_KEY, SCHEMA_LITERAL), ImmutableMap.of()));
    }

    @Test
    void testResolvesColumnsFromSchemaLiteralInSerdeParameters()
            throws Exception
    {
        assertResolvesEnvelope(avroTable(ImmutableMap.of(), ImmutableMap.of(AVRO_SCHEMA_LITERAL_KEY, SCHEMA_LITERAL)));
    }

    @Test
    void testResolvesColumnsFromSchemaUrl(@TempDir Path tempDir)
            throws Exception
    {
        Path schemaFile = tempDir.resolve("envelope.avsc");
        Files.writeString(schemaFile, SCHEMA_LITERAL);

        assertResolvesEnvelope(avroTable(ImmutableMap.of(AVRO_SCHEMA_URL_KEY, "local://" + schemaFile), ImmutableMap.of()));
    }

    @Test
    void testResolvedTypesAndComments()
            throws Exception
    {
        Table table = avroTable(ImmutableMap.of(AVRO_SCHEMA_LITERAL_KEY, SCHEMA_LITERAL), ImmutableMap.of());
        List<Column> columns = withColumnsFromAvroSchema(FILE_SYSTEM, table).getDataColumns();

        assertThat(columns.get(0).getType()).isEqualTo(HIVE_STRING);
        assertThat(columns.get(0).getComment()).contains("the id");
        // nullable union [null, int] unwraps to the underlying type
        assertThat(columns.get(1).getType()).isEqualTo(HIVE_INT);
    }

    @Test
    void testMapsLogicalAndComplexTypes()
            throws Exception
    {
        String literal =
                """
                {
                  "type": "record",
                  "name": "Types",
                  "fields": [
                    {"name": "a_date", "type": {"type": "int", "logicalType": "date"}},
                    {"name": "a_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                    {"name": "a_decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                    {"name": "a_varchar", "type": {"type": "string", "logicalType": "varchar", "maxLength": 12}},
                    {"name": "an_array", "type": {"type": "array", "items": "string"}},
                    {"name": "a_map", "type": {"type": "map", "values": "long"}},
                    {"name": "a_record", "type": {"type": "record", "name": "Nested", "fields": [{"name": "x", "type": "double"}]}},
                    {"name": "an_enum", "type": {"type": "enum", "name": "Colour", "symbols": ["RED", "GREEN"]}},
                    {"name": "a_fixed", "type": {"type": "fixed", "name": "Md5", "size": 16}}
                  ]
                }
                """;
        Table table = avroTable(ImmutableMap.of(AVRO_SCHEMA_LITERAL_KEY, literal), ImmutableMap.of());

        // These are the mappings the Avro read path itself produces, since the resolver reuses its
        // type handler. Notably ENUM and FIXED resolve rather than failing the whole table.
        assertThat(withColumnsFromAvroSchema(FILE_SYSTEM, table).getDataColumns())
                .extracting(column -> column.getName() + ":" + column.getType())
                .containsExactly(
                        "a_date:date",
                        "a_timestamp:timestamp",
                        "a_decimal:decimal(10,2)",
                        "a_varchar:varchar(12)",
                        "an_array:array<string>",
                        "a_map:map<string,bigint>",
                        "a_record:struct<x:double>",
                        "an_enum:string",
                        "a_fixed:binary");
    }

    @Test
    void testExcludesPartitionColumns()
            throws Exception
    {
        // The Thrift path gets this for free because get_fields excludes partition keys. If the Avro
        // schema does list the partition column, it must not also be reported as a data column.
        String literal =
                """
                {
                  "type": "record",
                  "name": "Envelope",
                  "fields": [
                    {"name": "event_id", "type": "string"},
                    {"name": "acquisition_date", "type": "string"}
                  ]
                }
                """;
        Table table = Table.builder(avroTable(ImmutableMap.of(AVRO_SCHEMA_LITERAL_KEY, literal), ImmutableMap.of()))
                .setPartitionColumns(ImmutableList.of(new Column("acquisition_date", HIVE_STRING, Optional.empty(), ImmutableMap.of())))
                .build();

        Table resolved = withColumnsFromAvroSchema(FILE_SYSTEM, table);
        assertThat(resolved.getDataColumns()).extracting(Column::getName).containsExactly("event_id");
        assertThat(resolved.getPartitionColumns()).extracting(Column::getName).containsExactly("acquisition_date");
    }

    @Test
    void testLowercasesColumnNames()
            throws Exception
    {
        // Hive column names are lower case, and the Thrift path returns lower case names because
        // Hive's Avro SerDe lower cases them when producing field schemas
        String literal =
                """
                {
                  "type": "record",
                  "name": "Envelope",
                  "fields": [{"name": "feedbackTimestamp", "type": "string"}]
                }
                """;
        Table table = avroTable(ImmutableMap.of(AVRO_SCHEMA_LITERAL_KEY, literal), ImmutableMap.of());

        assertThat(withColumnsFromAvroSchema(FILE_SYSTEM, table).getDataColumns())
                .extracting(Column::getName)
                .containsExactly("feedbacktimestamp");
    }

    @Test
    void testUnresolvableSchemaThrows()
    {
        Table missingFile = avroTable(ImmutableMap.of(AVRO_SCHEMA_URL_KEY, "local:///does/not/exist.avsc"), ImmutableMap.of());
        assertThat(isAvroTableWithSchemaSet(missingFile)).isTrue();
        assertThatThrownBy(() -> withColumnsFromAvroSchema(FILE_SYSTEM, missingFile))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("local:///does/not/exist.avsc");

        // A schema url that is not a valid location fails as an IllegalArgumentException rather than an IOException
        Table malformedUrl = avroTable(ImmutableMap.of(AVRO_SCHEMA_URL_KEY, "not a valid location"), ImmutableMap.of());
        assertThatThrownBy(() -> withColumnsFromAvroSchema(FILE_SYSTEM, malformedUrl))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No scheme for file system location: not a valid location");
    }

    @Test
    void testIsAvroTableWithSchemaSet()
    {
        // Avro with no schema set: the stored columns remain authoritative
        assertThat(isAvroTableWithSchemaSet(avroTable(ImmutableMap.of(), ImmutableMap.of()))).isFalse();

        // Non-Avro tables are never touched, even if they somehow carry the property
        Table parquetTable = Table.builder(avroTable(ImmutableMap.of(AVRO_SCHEMA_LITERAL_KEY, SCHEMA_LITERAL), ImmutableMap.of()))
                .withStorage(storage -> storage.setStorageFormat(PARQUET.toStorageFormat()))
                .build();
        assertThat(isAvroTableWithSchemaSet(parquetTable)).isFalse();
    }

    private static void assertResolvesEnvelope(Table table)
            throws Exception
    {
        assertThat(isAvroTableWithSchemaSet(table)).isTrue();
        assertThat(withColumnsFromAvroSchema(FILE_SYSTEM, table).getDataColumns())
                .extracting(Column::getName)
                .containsExactly("event_id", "amount");
    }

    private static Table avroTable(Map<String, String> parameters, Map<String, String> serdeParameters)
    {
        return Table.builder()
                .setDatabaseName("test_db")
                .setTableName("event_topic")
                .setOwner(Optional.empty())
                .setTableType("EXTERNAL_TABLE")
                .setDataColumns(STORED_COLUMNS)
                .setParameters(parameters)
                .withStorage(storage -> storage
                        .setStorageFormat(AVRO.toStorageFormat())
                        .setLocation("/tmp/test_db/test_table")
                        .setSerdeParameters(serdeParameters))
                .build();
    }
}
