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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public final class ParquetTypeUtils
{
    private ParquetTypeUtils() {}

    public static List<PrimitiveColumnIO> getColumns(MessageType fileSchema, MessageType requestedSchema)
    {
        return ImmutableList.copyOf(new ColumnIOFactory().getColumnIO(requestedSchema, fileSchema, true).getLeaves());
    }

    public static MessageColumnIO getColumnIO(MessageType fileSchema, MessageType requestedSchema)
    {
        return new ColumnIOFactory().getColumnIO(requestedSchema, fileSchema, true);
    }

    public static GroupColumnIO getMapKeyValueColumn(GroupColumnIO groupColumnIO)
    {
        while (groupColumnIO.getChildrenCount() == 1) {
            groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
        }
        return groupColumnIO;
    }

    /* For backward-compatibility, the type of elements in LIST-annotated structures should always be determined by the following rules:
     * 1. If the repeated field is not a group, then its type is the element type and elements are required.
     * 2. If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
     * 3. If the repeated field is a group with one field and is named either array or uses the LIST-annotated group's name with _tuple appended then the repeated type is the element type and elements are required.
     * 4. Otherwise, the repeated field's type is the element type with the repeated field's repetition.
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
     */
    public static ColumnIO getArrayElementColumn(ColumnIO columnIO)
    {
        while (columnIO instanceof GroupColumnIO && !columnIO.getType().isRepetition(REPEATED)) {
            columnIO = ((GroupColumnIO) columnIO).getChild(0);
        }

        /* If array has a standard 3-level structure with middle level repeated group with a single field:
         *  optional group my_list (LIST) {
         *     repeated group element {
         *        required binary str (UTF8);
         *     };
         *  }
         */
        if (columnIO instanceof GroupColumnIO &&
                columnIO.getType().getLogicalTypeAnnotation() == null &&
                ((GroupColumnIO) columnIO).getChildrenCount() == 1 &&
                !columnIO.getName().equals("array") &&
                !columnIO.getName().equals(columnIO.getParent().getName() + "_tuple")) {
            return ((GroupColumnIO) columnIO).getChild(0);
        }

        /* Backward-compatibility support for 2-level arrays where a repeated field is not a group:
         *   optional group my_list (LIST) {
         *      repeated int32 element;
         *   }
         */
        return columnIO;
    }

    public static Map<List<String>, ColumnDescriptor> getDescriptors(MessageType fileSchema, MessageType requestedSchema)
    {
        // io.trino.parquet.reader.MetadataReader.readFooter performs lower casing of all column names in fileSchema.
        // requestedSchema also contains lower cased columns because of being derived from fileSchema.
        // io.trino.parquet.ParquetTypeUtils.getParquetTypeByName takes care of case-insensitive matching if needed.
        // Therefore, we don't need to repeat case-insensitive matching here.
        return getColumns(fileSchema, requestedSchema)
                .stream()
                .collect(toImmutableMap(
                        columnIO -> Arrays.asList(columnIO.getFieldPath()),
                        PrimitiveColumnIO::getColumnDescriptor,
                        // Same column name may occur more than once when the file is written by case-sensitive tools
                        (oldValue, _) -> oldValue));
    }

    @SuppressWarnings("deprecation")
    public static ParquetEncoding getParquetEncoding(Encoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> ParquetEncoding.PLAIN;
            case RLE -> ParquetEncoding.RLE;
            case BYTE_STREAM_SPLIT -> ParquetEncoding.BYTE_STREAM_SPLIT;
            case BIT_PACKED -> ParquetEncoding.BIT_PACKED;
            case PLAIN_DICTIONARY -> ParquetEncoding.PLAIN_DICTIONARY;
            case DELTA_BINARY_PACKED -> ParquetEncoding.DELTA_BINARY_PACKED;
            case DELTA_LENGTH_BYTE_ARRAY -> ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
            case DELTA_BYTE_ARRAY -> ParquetEncoding.DELTA_BYTE_ARRAY;
            case RLE_DICTIONARY -> ParquetEncoding.RLE_DICTIONARY;
        };
    }

    public static org.apache.parquet.schema.Type getParquetTypeByName(String columnName, GroupType groupType)
    {
        if (groupType.containsField(columnName)) {
            return groupType.getType(columnName);
        }
        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (org.apache.parquet.schema.Type type : groupType.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }

        return null;
    }

    /**
     * Parquet column names are case-sensitive unlike Hive, which converts all column names to lowercase.
     * Therefore, when we look up columns we first check for exact match, and if that fails we look for a case-insensitive match.
     */
    public static ColumnIO lookupColumnByName(GroupColumnIO groupColumnIO, String columnName)
    {
        ColumnIO columnIO = groupColumnIO.getChild(columnName);

        if (columnIO != null) {
            return columnIO;
        }

        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            if (groupColumnIO.getChild(i).getName().equalsIgnoreCase(columnName)) {
                return groupColumnIO.getChild(i);
            }
        }

        return null;
    }

    @Nullable
    public static ColumnIO lookupColumnById(GroupColumnIO groupColumnIO, int columnId)
    {
        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            ColumnIO child = groupColumnIO.getChild(i);
            if (child.getType().getId().intValue() == columnId) {
                return child;
            }
        }
        return null;
    }

    public static Optional<DecimalType> createDecimalType(PrimitiveField field)
    {
        if (!(field.getDescriptor().getPrimitiveType().getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation decimalLogicalType)) {
            return Optional.empty();
        }
        return Optional.of(DecimalType.createDecimalType(decimalLogicalType.getPrecision(), decimalLogicalType.getScale()));
    }

    /**
     * For optional fields:
     * <ul>
     * <li>definitionLevel == maxDefinitionLevel     =&gt; Value is defined</li>
     * <li>definitionLevel == maxDefinitionLevel - 1 =&gt; Value is null</li>
     * <li>definitionLevel &lt; maxDefinitionLevel - 1  =&gt; Value does not exist, because one of its optional parent fields is null</li>
     * </ul>
     */
    public static boolean isValueNull(boolean required, int definitionLevel, int maxDefinitionLevel)
    {
        return !required && (definitionLevel == maxDefinitionLevel - 1);
    }

    public static boolean isOptionalFieldValueNull(int definitionLevel, int maxDefinitionLevel)
    {
        return definitionLevel == maxDefinitionLevel - 1;
    }

    public static long getShortDecimalValue(byte[] bytes)
    {
        return getShortDecimalValue(bytes, 0, bytes.length);
    }

    public static long getShortDecimalValue(byte[] bytes, int startOffset, int length)
    {
        long value = 0;
        switch (length) {
            case 8:
                value |= bytes[startOffset + 7] & 0xFFL;
                // fall through
            case 7:
                value |= (bytes[startOffset + 6] & 0xFFL) << 8;
                // fall through
            case 6:
                value |= (bytes[startOffset + 5] & 0xFFL) << 16;
                // fall through
            case 5:
                value |= (bytes[startOffset + 4] & 0xFFL) << 24;
                // fall through
            case 4:
                value |= (bytes[startOffset + 3] & 0xFFL) << 32;
                // fall through
            case 3:
                value |= (bytes[startOffset + 2] & 0xFFL) << 40;
                // fall through
            case 2:
                value |= (bytes[startOffset + 1] & 0xFFL) << 48;
                // fall through
            case 1:
                value |= (bytes[startOffset] & 0xFFL) << 56;
        }
        value = value >> ((8 - length) * 8);
        return value;
    }

    public static void checkBytesFitInShortDecimal(byte[] bytes, int offset, int length, ColumnDescriptor descriptor)
    {
        int endOffset = offset + length;
        // Equivalent to expectedValue = bytes[endOffset] < 0 ? -1 : 0
        byte expectedValue = (byte) (bytes[endOffset] >> 7);
        for (int i = offset; i < endOffset; i++) {
            if (bytes[i] != expectedValue) {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Could not read unscaled value %s into a short decimal from column %s",
                        new BigInteger(bytes, offset, length + Long.BYTES),
                        descriptor));
            }
        }
    }

    public static byte[] paddingBigInteger(BigInteger bigInteger, int numBytes)
    {
        byte[] bytes = bigInteger.toByteArray();
        if (bytes.length == numBytes) {
            return bytes;
        }
        byte[] result = new byte[numBytes];
        if (bigInteger.signum() < 0) {
            Arrays.fill(result, 0, numBytes - bytes.length, (byte) 0xFF);
        }
        System.arraycopy(bytes, 0, result, numBytes - bytes.length, bytes.length);
        return result;
    }

    /**
     * Assumes the parent of columnIO is a MessageColumnIO, i.e. columnIO should be a top level column in the schema.
     */
    public static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        return constructField(type, columnIO, true);
    }

    private static Optional<Field> constructField(Type type, ColumnIO columnIO, boolean isTopLevel)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        if (isVariantType(type, columnIO)) {
            checkArgument(type.getTypeParameters().isEmpty(), "Expected type parameters to be empty for variant but got %s", type.getTypeParameters());
            if (!(columnIO instanceof GroupColumnIO groupColumnIo)) {
                throw new IllegalStateException("Expected columnIO to be GroupColumnIO but got %s".formatted(columnIO.getClass().getSimpleName()));
            }
            Field valueField = constructField(VARBINARY, groupColumnIo.getChild(0), false).orElseThrow();
            Field metadataField = constructField(VARBINARY, groupColumnIo.getChild(1), false).orElseThrow();
            return Optional.of(new VariantField(type, repetitionLevel, definitionLevel, required, valueField, metadataField));
        }
        if (type instanceof RowType rowType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (RowType.Field rowField : fields) {
                String name = rowField.getName().orElseThrow().toLowerCase(Locale.ENGLISH);
                Optional<Field> field = constructField(rowField.getType(), lookupColumnByName(groupColumnIO, name), false);
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType mapType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0), false);
            Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1), false);
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType arrayType) {
            // Per the parquet spec (https://github.com/apache/parquet-format/blob/master/LogicalTypes.md):
            // `A repeated field that is neither contained by a LIST- or MAP-annotated group nor annotated by LIST or MAP should be interpreted as a required list of required elements
            // where the element type is the type of the field.`
            //
            // A parquet encoding for a required list of strings can be expressed in two ways, however for backwards compatibility they should be handled the same, so here we need
            // to adjust repetition and definition levels when converting ColumnIOs to Fields.
            //      1. required group colors (LIST) {
            //              repeated group list {
            //                  required string element;
            //              }
            //         }
            //      2. repeated binary colors (STRING);
            if (columnIO instanceof PrimitiveColumnIO primitiveColumnIO) {
                if (columnIO.getType().getRepetition() != REPEATED || repetitionLevel == 0 || definitionLevel == 0) {
                    throw new TrinoException(NOT_SUPPORTED, format("Unsupported schema for Parquet column (%s)", primitiveColumnIO.getColumnDescriptor()));
                }
                PrimitiveField primitiveFieldElement = new PrimitiveField(arrayType.getElementType(), true, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId());
                return Optional.of(new GroupField(type, repetitionLevel - 1, definitionLevel - 1, true, ImmutableList.of(Optional.of(primitiveFieldElement))));
            }
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            Optional<Field> field = constructField(arrayType.getElementType(), getArrayElementColumn(groupColumnIO.getChild(0)), false);
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        if (primitiveColumnIO.getType().getRepetition() == REPEATED && isTopLevel) {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", type, primitiveColumnIO.getColumnDescriptor()));
        }
        return Optional.of(new PrimitiveField(type, required, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId()));
    }

    private static boolean isVariantType(Type type, ColumnIO columnIO)
    {
        return type.getTypeSignature().getBase().equals(JSON) &&
                columnIO instanceof GroupColumnIO groupColumnIo &&
                groupColumnIo.getChildrenCount() == 2 &&
                groupColumnIo.getChild("value") != null &&
                groupColumnIo.getChild("metadata") != null;
    }
}
