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
package io.trino.plugin.hive.avro;

import com.google.common.base.Splitter;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager;
import io.trino.metastore.HiveType;
import io.trino.metastore.type.CharTypeInfo;
import io.trino.metastore.type.DecimalTypeInfo;
import io.trino.metastore.type.ListTypeInfo;
import io.trino.metastore.type.MapTypeInfo;
import io.trino.metastore.type.PrimitiveCategory;
import io.trino.metastore.type.PrimitiveTypeInfo;
import io.trino.metastore.type.StructTypeInfo;
import io.trino.metastore.type.TypeInfo;
import io.trino.metastore.type.UnionTypeInfo;
import io.trino.metastore.type.VarcharTypeInfo;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.hive.formats.avro.AvroHiveConstants.CHAR_TYPE_LOGICAL_NAME;
import static io.trino.hive.formats.avro.AvroHiveConstants.SCHEMA_DOC;
import static io.trino.hive.formats.avro.AvroHiveConstants.SCHEMA_LITERAL;
import static io.trino.hive.formats.avro.AvroHiveConstants.SCHEMA_NAME;
import static io.trino.hive.formats.avro.AvroHiveConstants.SCHEMA_NAMESPACE;
import static io.trino.hive.formats.avro.AvroHiveConstants.SCHEMA_NONE;
import static io.trino.hive.formats.avro.AvroHiveConstants.SCHEMA_URL;
import static io.trino.hive.formats.avro.AvroHiveConstants.TABLE_NAME;
import static io.trino.hive.formats.avro.AvroHiveConstants.VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP;
import static io.trino.hive.formats.avro.AvroHiveConstants.VARCHAR_TYPE_LOGICAL_NAME;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_COMMENTS;
import static java.util.Collections.emptyList;
import static java.util.function.Predicate.not;
import static java.util.function.UnaryOperator.identity;

public final class AvroHiveFileUtils
{
    private final AtomicInteger recordNameSuffix = new AtomicInteger(0);

    private AvroHiveFileUtils() {}

    // Lifted and shifted from org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.determineSchemaOrThrowException
    public static Schema determineSchemaOrThrowException(TrinoFileSystem fileSystem, Map<String, String> properties)
            throws IOException
    {
        // Try pull schema from literal table property
        String schemaString = properties.getOrDefault(SCHEMA_LITERAL, "");
        if (!schemaString.isBlank() && !schemaString.equals(SCHEMA_NONE)) {
            return getSchemaParser().parse(schemaString);
        }

        // Try pull schema directly from URL
        String schemaURL = properties.getOrDefault(SCHEMA_URL, "");
        if (!schemaURL.isBlank()) {
            TrinoInputFile schemaFile = fileSystem.newInputFile(Location.of(schemaURL));
            if (!schemaFile.exists()) {
                throw new IOException("No avro schema file not found at " + schemaURL);
            }
            try (TrinoInputStream inputStream = schemaFile.newStream()) {
                return getSchemaParser().parse(inputStream);
            }
            catch (IOException e) {
                throw new IOException("Unable to read avro schema file from given path: " + schemaURL, e);
            }
        }
        return getSchemaFromProperties(properties);
    }

    private static Schema getSchemaFromProperties(Map<String, String> schema)
            throws IOException
    {
        List<String> columnNames = getColumnNames(schema);
        List<HiveType> columnTypes = getColumnTypes(schema);
        if (columnNames.isEmpty() || columnTypes.isEmpty()) {
            throw new IOException("Unable to parse column names or column types from schema to create Avro Schema");
        }
        if (columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("Avro Schema initialization failed. Number of column name and column type differs. columnNames = %s, columnTypes = %s".formatted(columnNames, columnTypes));
        }
        List<String> columnComments = Optional.ofNullable(schema.get(LIST_COLUMN_COMMENTS))
                .filter(not(String::isBlank))
                .map(Splitter.on('\0')::splitToList)
                .orElse(emptyList());

        String tableName = schema.get(TABLE_NAME);
        String tableComment = schema.get(TABLE_COMMENT);

        return constructSchemaFromParts(
                columnNames,
                columnTypes,
                columnComments,
                Optional.ofNullable(schema.get(SCHEMA_NAMESPACE)),
                Optional.ofNullable(schema.getOrDefault(SCHEMA_NAME, tableName)),
                Optional.ofNullable(schema.getOrDefault(SCHEMA_DOC, tableComment)));
    }

    private static Schema constructSchemaFromParts(List<String> columnNames, List<HiveType> columnTypes,
            List<String> columnComments, Optional<String> namespace, Optional<String> name, Optional<String> doc)
    {
        // create instance of this class to keep nested record naming consistent for any given inputs
        AvroHiveFileUtils recordIncrementingUtil = new AvroHiveFileUtils();
        SchemaBuilder.RecordBuilder<Schema> schemaBuilder = SchemaBuilder.record(name.orElse("baseRecord"));
        namespace.ifPresent(schemaBuilder::namespace);
        doc.ifPresent(schemaBuilder::doc);
        SchemaBuilder.FieldAssembler<Schema> fieldBuilder = schemaBuilder.fields();

        for (int i = 0; i < columnNames.size(); ++i) {
            String comment = columnComments.size() > i ? columnComments.get(i) : null;
            Schema fieldSchema = recordIncrementingUtil.avroSchemaForHiveType(columnTypes.get(i));
            fieldBuilder = fieldBuilder
                    .name(columnNames.get(i))
                    .doc(comment)
                    .type(fieldSchema)
                    .withDefault(null);
        }
        return fieldBuilder.endRecord();
    }

    private Schema avroSchemaForHiveType(HiveType hiveType)
    {
        Schema schema = switch (hiveType.getCategory()) {
            case PRIMITIVE -> createAvroPrimitive(hiveType);
            case LIST -> {
                ListTypeInfo listTypeInfo = (ListTypeInfo) hiveType.getTypeInfo();
                yield Schema.createArray(avroSchemaForHiveType(HiveType.fromTypeInfo(listTypeInfo.getListElementTypeInfo())));
            }
            case MAP -> {
                MapTypeInfo mapTypeInfo = ((MapTypeInfo) hiveType.getTypeInfo());
                TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
                if (!(keyTypeInfo instanceof PrimitiveTypeInfo primitiveKeyTypeInfo) ||
                        primitiveKeyTypeInfo.getPrimitiveCategory() != PrimitiveCategory.STRING) {
                    throw new UnsupportedOperationException("Key of Map must be a String");
                }
                TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
                yield Schema.createMap(avroSchemaForHiveType(HiveType.fromTypeInfo(valueTypeInfo)));
            }
            case STRUCT -> createAvroRecord(hiveType);
            case UNION -> {
                List<Schema> childSchemas = new ArrayList<>();
                for (TypeInfo childTypeInfo : ((UnionTypeInfo) hiveType.getTypeInfo()).getAllUnionObjectTypeInfos()) {
                    final Schema childSchema = avroSchemaForHiveType(HiveType.fromTypeInfo(childTypeInfo));
                    if (childSchema.getType() == Schema.Type.UNION) {
                        childSchemas.addAll(childSchema.getTypes());
                    }
                    else {
                        childSchemas.add(childSchema);
                    }
                }
                yield Schema.createUnion(removeDuplicateNullSchemas(childSchemas));
            }
        };

        return wrapInUnionWithNull(schema);
    }

    private static Schema createAvroPrimitive(HiveType hiveType)
    {
        if (!(hiveType.getTypeInfo() instanceof PrimitiveTypeInfo primitiveTypeInfo)) {
            throw new IllegalStateException("HiveType in primitive category must have PrimitiveTypeInfo");
        }
        return switch (primitiveTypeInfo.getPrimitiveCategory()) {
            case STRING -> Schema.create(Schema.Type.STRING);
            case CHAR -> {
                Schema charSchema = SchemaBuilder.builder().type(Schema.create(Schema.Type.STRING));
                charSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, CHAR_TYPE_LOGICAL_NAME);
                charSchema.addProp(VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP, ((CharTypeInfo) hiveType.getTypeInfo()).getLength());
                yield charSchema;
            }
            case VARCHAR -> {
                Schema varcharSchema = SchemaBuilder.builder().type(Schema.create(Schema.Type.STRING));
                varcharSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, VARCHAR_TYPE_LOGICAL_NAME);
                varcharSchema.addProp(VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP, ((VarcharTypeInfo) hiveType.getTypeInfo()).getLength());
                yield varcharSchema;
            }
            case BINARY -> Schema.create(Schema.Type.BYTES);
            case BYTE, SHORT, INT -> Schema.create(Schema.Type.INT);
            case LONG -> Schema.create(Schema.Type.LONG);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case DECIMAL -> {
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) hiveType.getTypeInfo();
                LogicalTypes.Decimal decimalLogicalType = LogicalTypes.decimal(decimalTypeInfo.precision(), decimalTypeInfo.scale());
                yield decimalLogicalType.addToSchema(Schema.create(Schema.Type.BYTES));
            }
            case DATE -> NativeLogicalTypesAvroTypeManager.DATE_SCHEMA;
            case TIMESTAMP -> NativeLogicalTypesAvroTypeManager.TIMESTAMP_MILLIS_SCHEMA;
            case VOID -> Schema.create(Schema.Type.NULL);
            default -> throw new UnsupportedOperationException(hiveType + " is not supported.");
        };
    }

    private Schema createAvroRecord(HiveType hiveType)
    {
        if (!(hiveType.getTypeInfo() instanceof StructTypeInfo structTypeInfo)) {
            throw new IllegalStateException("HiveType type info must be Struct Type info to make Avro Record");
        }

        final List<String> allStructFieldNames =
                structTypeInfo.getAllStructFieldNames();
        final List<TypeInfo> allStructFieldTypeInfo =
                structTypeInfo.getAllStructFieldTypeInfos();
        if (allStructFieldNames.size() != allStructFieldTypeInfo.size()) {
            throw new IllegalArgumentException("Failed to generate avro schema from hive schema. " +
                    "name and column type differs. names = " + allStructFieldNames + ", types = " +
                    allStructFieldTypeInfo);
        }

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
                .record("record_" + recordNameSuffix.getAndIncrement())
                .doc(structTypeInfo.toString())
                .fields();

        for (int i = 0; i < allStructFieldNames.size(); ++i) {
            final TypeInfo childTypeInfo = allStructFieldTypeInfo.get(i);
            final Schema fieldSchema = avroSchemaForHiveType(HiveType.fromTypeInfo(childTypeInfo));
            fieldAssembler = fieldAssembler
                    .name(allStructFieldNames.get(i))
                    .doc(childTypeInfo.toString())
                    .type(fieldSchema)
                    .withDefault(null);
        }
        return fieldAssembler.endRecord();
    }

    public static Schema wrapInUnionWithNull(Schema schema)
    {
        return switch (schema.getType()) {
            case NULL -> schema;
            case UNION -> Schema.createUnion(removeDuplicateNullSchemas(schema.getTypes()));
            default -> Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
        };
    }

    private static List<Schema> removeDuplicateNullSchemas(List<Schema> childSchemas)
    {
        List<Schema> prunedSchemas = new ArrayList<>();
        boolean isNullPresent = false;
        for (Schema schema : childSchemas) {
            if (schema.getType() == Schema.Type.NULL) {
                isNullPresent = true;
            }
            else {
                prunedSchemas.add(schema);
            }
        }
        if (isNullPresent) {
            prunedSchemas.add(0, Schema.create(Schema.Type.NULL));
        }

        return prunedSchemas;
    }

    static Map<String, String> getCanonicalToGivenFieldName(Schema schema)
    {
        // Lower case top level fields to allow for manually set avro schema (passed in via avro_schema_literal or avro_schema_url) to have uppercase field names
        return schema.getFields().stream()
                .map(Schema.Field::name)
                .collect(toImmutableMap(fieldName -> fieldName.toLowerCase(Locale.ENGLISH), identity()));
    }

    private static Schema.Parser getSchemaParser()
    {
        // HIVE-24797: Disable validate default values when parsing Avro schemas.
        return new Schema.Parser().setValidateDefaults(false);
    }
}
