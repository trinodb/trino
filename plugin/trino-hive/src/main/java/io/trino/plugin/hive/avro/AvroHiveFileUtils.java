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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.type.CharTypeInfo;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.PrimitiveCategory;
import io.trino.plugin.hive.type.PrimitiveTypeInfo;
import io.trino.plugin.hive.type.StructTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.type.UnionTypeInfo;
import io.trino.plugin.hive.type.VarcharTypeInfo;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.avro.AvroHiveConstants.AVRO_SERDE_SCHEMA;
import static io.trino.plugin.hive.avro.AvroHiveConstants.CHAR_TYPE_LOGICAL_NAME;
import static io.trino.plugin.hive.avro.AvroHiveConstants.DATE_TYPE_NAME;
import static io.trino.plugin.hive.avro.AvroHiveConstants.SCHEMA_DOC;
import static io.trino.plugin.hive.avro.AvroHiveConstants.SCHEMA_LITERAL;
import static io.trino.plugin.hive.avro.AvroHiveConstants.SCHEMA_NAME;
import static io.trino.plugin.hive.avro.AvroHiveConstants.SCHEMA_NAMESPACE;
import static io.trino.plugin.hive.avro.AvroHiveConstants.SCHEMA_NONE;
import static io.trino.plugin.hive.avro.AvroHiveConstants.SCHEMA_URL;
import static io.trino.plugin.hive.avro.AvroHiveConstants.TABLE_NAME;
import static io.trino.plugin.hive.avro.AvroHiveConstants.TIMESTAMP_TYPE_NAME;
import static io.trino.plugin.hive.avro.AvroHiveConstants.VARCHAR_TYPE_LOGICAL_NAME;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_COMMENTS;
import static java.util.function.Predicate.not;

public final class AvroHiveFileUtils
{
    private AvroHiveFileUtils() {}

    // Lifted and shifted from org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.determineSchemaOrThrowException
    public static Schema determineSchemaOrThrowException(TrinoFileSystem fileSystem, Configuration conf, Properties properties)
            throws IOException
    {
        // Try pull schema from literal table property
        String schemaString = properties.getProperty(SCHEMA_LITERAL, "");
        if (!schemaString.isBlank() && !schemaString.equals(SCHEMA_NONE)) {
            return getSchemaParser().parse(schemaString);
        }

        // Try pull schema directly from URL
        String schemaURL = properties.getProperty(SCHEMA_URL, "");
        if (!schemaURL.isBlank()) {
            TrinoInputFile schemaFile = fileSystem.newInputFile(schemaURL);
            if (!schemaFile.exists()) {
                throw new IOException("No avro schema file not found at " + schemaURL);
            }
            try (TrinoInputStream inputStream = schemaFile.newStream()) {
                return getSchemaParser().parse(inputStream);
            }
            catch (IOException ioException) {
                throw new IOException("Unable to read avro schema file from given path: " + schemaURL, ioException);
            }
        }
        else {
            Schema schema = getSchemaFromProperties(properties);
            properties.setProperty(SCHEMA_LITERAL, schema.toString());
            if (conf != null) {
                conf.set(AVRO_SERDE_SCHEMA, schema.toString(false));
            }
            return schema;
        }
    }

    private static Schema getSchemaFromProperties(Properties properties)
            throws IOException
    {
        List<String> columnNames = getColumnNames(properties);
        List<HiveType> columnTypes = getColumnTypes(properties);
        if (columnNames.isEmpty() || columnTypes.isEmpty()) {
            throw new IOException("Unable to parse column names or column types from job properties to create Avro Schema");
        }
        if (columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("Avro Schema initialization failed. Number of column " +
                    "name and column type differs. columnNames = " + columnNames + ", columnTypes = " +
                    columnTypes);
        }
        List<String> columnComments = Optional.ofNullable(properties.getProperty(LIST_COLUMN_COMMENTS))
                .filter(not(String::isBlank))
                .map(property -> property.split("\0"))
                .map(Arrays::asList)
                .orElse(new ArrayList<String>());

        final String tableName = properties.getProperty(TABLE_NAME);
        final String tableComment = properties.getProperty(TABLE_COMMENT);

        return constructSchemaFromParts(
                columnNames,
                columnTypes,
                columnComments,
                Optional.ofNullable(properties.getProperty(SCHEMA_NAMESPACE)),
                Optional.ofNullable(properties.getProperty(SCHEMA_NAME, tableName)),
                Optional.ofNullable(properties.getProperty(SCHEMA_DOC, tableComment)));
    }

    private static Schema constructSchemaFromParts(List<String> columnNames, List<HiveType> columnTypes,
            List<String> columnComments, Optional<String> namespace, Optional<String> name, Optional<String> doc)
    {
        SchemaBuilder.RecordBuilder<Schema> schemaBuilder = SchemaBuilder.record(name.orElse("baseRecord"));
        namespace.ifPresent(schemaBuilder::namespace);
        doc.ifPresent(schemaBuilder::doc);
        SchemaBuilder.FieldAssembler<Schema> fieldBuilder = schemaBuilder.fields();

        for (int i = 0; i < columnNames.size(); ++i) {
            String comment = columnComments.size() > i ? columnComments.get(i) : null;
            Schema fieldSchema = avroSchemaForHiveType(columnTypes.get(i));
            // flatten one level like done in org.apache.hadoop.hive.serde2.avro.TypeInfoToSchema.getFields
            fieldBuilder = addFieldToAssembler(fieldBuilder, columnNames.get(i), comment, fieldSchema);
        }
        return fieldBuilder.endRecord();
    }

    private static Schema avroSchemaForHiveType(HiveType hiveType)
    {
        Schema schema = switch (hiveType.getCategory()) {
            case PRIMITIVE -> createAvroPrimitive(hiveType);
            case LIST -> {
                ListTypeInfo listTypeInfo = (ListTypeInfo) hiveType.getTypeInfo();
                yield Schema.createArray(avroSchemaForHiveType(HiveType.toHiveType(listTypeInfo.getListElementTypeInfo())));
            }
            case MAP -> {
                MapTypeInfo mapTypeInfo = ((MapTypeInfo) hiveType.getTypeInfo());
                TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
                if (!(keyTypeInfo instanceof PrimitiveTypeInfo primitiveKeyTypeInfo) ||
                        primitiveKeyTypeInfo.getPrimitiveCategory() != PrimitiveCategory.STRING) {
                    throw new UnsupportedOperationException("Key of Map can only be a String");
                }
                TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
                yield Schema.createMap(avroSchemaForHiveType(HiveType.toHiveType(valueTypeInfo)));
            }
            case STRUCT -> createAvroRecord(hiveType);
            case UNION -> {
                List<Schema> childSchemas = new ArrayList<Schema>();
                for (TypeInfo childTypeInfo : ((UnionTypeInfo) hiveType.getTypeInfo()).getAllUnionObjectTypeInfos()) {
                    final Schema childSchema = avroSchemaForHiveType(HiveType.toHiveType(childTypeInfo));
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
            case CHAR -> getSchemaParser().parse("{" +
                    "\"type\":\"" + Schema.Type.STRING.getName() + "\"," +
                    "\"logicalType\":\"" + CHAR_TYPE_LOGICAL_NAME + "\"," +
                    "\"maxLength\":" + ((CharTypeInfo) hiveType.getTypeInfo()).getLength() + "}");
            case VARCHAR -> getSchemaParser().parse("{" +
                    "\"type\":\"" + Schema.Type.STRING.getName() + "\"," +
                    "\"logicalType\":\"" + VARCHAR_TYPE_LOGICAL_NAME + "\"," +
                    "\"maxLength\":" + ((VarcharTypeInfo) hiveType.getTypeInfo()).getLength() + "}");
            case BINARY -> Schema.create(Schema.Type.BYTES);
            case BYTE, SHORT, INT -> Schema.create(Schema.Type.INT);
            case LONG -> Schema.create(Schema.Type.LONG);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case DECIMAL -> {
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) hiveType.getTypeInfo();
                String precision = String.valueOf(decimalTypeInfo.precision());
                String scale = String.valueOf(decimalTypeInfo.scale());
                yield getSchemaParser().parse("{" +
                        "\"type\":\"bytes\"," +
                        "\"logicalType\":\"decimal\"," +
                        "\"precision\":" + precision + "," +
                        "\"scale\":" + scale + "}");
            }
            case DATE -> getSchemaParser().parse("{" +
                    "\"type\":\"" + Schema.Type.INT.getName() + "\"," +
                    "\"logicalType\":\"" + DATE_TYPE_NAME + "\"}");
            case TIMESTAMP -> getSchemaParser().parse("{" +
                    "\"type\":\"" + Schema.Type.LONG.getName() + "\"," +
                    "\"logicalType\":\"" + TIMESTAMP_TYPE_NAME + "\"}");
            case VOID -> Schema.create(Schema.Type.NULL);
            default -> throw new UnsupportedOperationException(hiveType + " is not supported.");
        };
    }

    private static Schema createAvroRecord(HiveType hiveType)
    {
        if (!(hiveType.getTypeInfo() instanceof StructTypeInfo structTypeInfo)) {
            throw new IllegalStateException("HiveType type info must be Struct Type info to make Avro Record");
        }

        final List<String> allStructFieldNames =
                structTypeInfo.getAllStructFieldNames();
        final List<TypeInfo> allStructFieldTypeInfos =
                structTypeInfo.getAllStructFieldTypeInfos();
        if (allStructFieldNames.size() != allStructFieldTypeInfos.size()) {
            throw new IllegalArgumentException("Failed to generate avro schema from hive schema. " +
                    "name and column type differs. names = " + allStructFieldNames + ", types = " +
                    allStructFieldTypeInfos);
        }

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
                .record("record_" + randomNameSuffix())
                .doc(structTypeInfo.toString())
                .fields();

        for (int i = 0; i < allStructFieldNames.size(); ++i) {
            final TypeInfo childTypeInfo = allStructFieldTypeInfos.get(i);
            final Schema grandChildSchemaField = avroSchemaForHiveType(HiveType.toHiveType(childTypeInfo));
            fieldAssembler = addFieldToAssembler(fieldAssembler, allStructFieldNames.get(i), childTypeInfo.toString(), grandChildSchemaField);
        }
        return fieldAssembler.endRecord();
    }

    private static SchemaBuilder.FieldAssembler<Schema> addFieldToAssembler(SchemaBuilder.FieldAssembler<Schema> fieldBuilder, String fieldName, String doc, Schema fieldSchema)
    {
        if (fieldSchema.getType() == Schema.Type.RECORD) {
            for (Schema.Field nestedField : fieldSchema.getFields()) {
                fieldBuilder = fieldBuilder
                        .name(nestedField.name())
                        .doc(nestedField.doc())
                        .type(nestedField.schema())
                        .withDefault(null);
            }
        }
        else {
            fieldBuilder = fieldBuilder
                    .name(fieldName)
                    .doc(doc)
                    .type(fieldSchema)
                    .withDefault(null);
        }
        return fieldBuilder;
    }

    public static Schema wrapInUnionWithNull(Schema schema)
    {
        return switch (schema.getType()) {
            case NULL -> schema;
            case UNION -> Schema.createUnion(removeDuplicateNullSchemas(schema.getTypes()));
            default -> Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
        };
    }

    //TODO lift from testing code?
    private static String randomNameSuffix()
    {
        Random random = new Random();
        char[] chars = new char[RANDOM_SUFFIX_LENGTH];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = ALPHABET[random.nextInt(0, ALPHABET.length)];
        }
        return new String(chars);
    }

    private static final int RANDOM_SUFFIX_LENGTH = 10;
    private static final char[] ALPHABET = new char[36];

    static {
        for (int digit = 0; digit < 10; digit++) {
            ALPHABET[digit] = (char) ('0' + digit);
        }

        for (int letter = 0; letter < 26; letter++) {
            ALPHABET[10 + letter] = (char) ('a' + letter);
        }
    }

    private static List<Schema> removeDuplicateNullSchemas(List<Schema> childSchemas)
    {
        List<Schema> prunedSchemas = new ArrayList<Schema>();
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

    private static Schema.Parser getSchemaParser()
    {
        // HIVE-24797: Disable validate default values when parsing Avro schemas.
        return new Schema.Parser().setValidateDefaults(false);
    }
}
