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
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public final class ParquetTypeUtils
{
    private ParquetTypeUtils() {}

    public static List<PrimitiveColumnIO> getColumns(MessageType fileSchema, MessageType requestedSchema)
    {
        return ImmutableList.copyOf((new ColumnIOFactory()).getColumnIO(requestedSchema, fileSchema, true).getLeaves());
    }

    public static MessageColumnIO getColumnIO(MessageType fileSchema, MessageType requestedSchema)
    {
        return (new ColumnIOFactory()).getColumnIO(requestedSchema, fileSchema, true);
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
        Map<List<String>, ColumnDescriptor> descriptorsByPath = new HashMap<>();
        List<PrimitiveColumnIO> columns = getColumns(fileSchema, requestedSchema);
        for (String[] paths : fileSchema.getPaths()) {
            List<String> columnPath = Arrays.asList(paths);
            getDescriptor(columns, columnPath)
                    .ifPresent(columnDescriptor -> descriptorsByPath.put(columnPath, columnDescriptor));
        }
        return descriptorsByPath;
    }

    public static Optional<ColumnDescriptor> getDescriptor(List<PrimitiveColumnIO> columns, List<String> path)
    {
        checkArgument(path.size() >= 1, "Parquet nested path should have at least one component");
        int index = getPathIndex(columns, path);
        if (index == -1) {
            return Optional.empty();
        }
        PrimitiveColumnIO columnIO = columns.get(index);
        return Optional.of(columnIO.getColumnDescriptor());
    }

    private static int getPathIndex(List<PrimitiveColumnIO> columns, List<String> path)
    {
        int maxLevel = path.size();
        int index = -1;
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            ColumnIO[] fields = columns.get(columnIndex).getPath();
            if (fields.length <= maxLevel) {
                continue;
            }
            if (fields[maxLevel].getName().equalsIgnoreCase(path.get(maxLevel - 1))) {
                boolean match = true;
                for (int level = 0; level < maxLevel - 1; level++) {
                    if (!fields[level + 1].getName().equalsIgnoreCase(path.get(level))) {
                        match = false;
                    }
                }

                if (match) {
                    index = columnIndex;
                }
            }
        }
        return index;
    }

    @SuppressWarnings("deprecation")
    public static ParquetEncoding getParquetEncoding(Encoding encoding)
    {
        switch (encoding) {
            case PLAIN:
                return ParquetEncoding.PLAIN;
            case RLE:
                return ParquetEncoding.RLE;
            case BYTE_STREAM_SPLIT:
                // TODO: https://github.com/trinodb/trino/issues/8357
                throw new ParquetDecodingException("Unsupported Parquet encoding: " + encoding);
            case BIT_PACKED:
                return ParquetEncoding.BIT_PACKED;
            case PLAIN_DICTIONARY:
                return ParquetEncoding.PLAIN_DICTIONARY;
            case DELTA_BINARY_PACKED:
                return ParquetEncoding.DELTA_BINARY_PACKED;
            case DELTA_LENGTH_BYTE_ARRAY:
                return ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
            case DELTA_BYTE_ARRAY:
                return ParquetEncoding.DELTA_BYTE_ARRAY;
            case RLE_DICTIONARY:
                return ParquetEncoding.RLE_DICTIONARY;
        }
        throw new ParquetDecodingException("Unsupported Parquet encoding: " + encoding);
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
     * definitionLevel == maxDefinitionLevel     => Value is defined
     * definitionLevel == maxDefinitionLevel - 1 => Value is null
     * definitionLevel < maxDefinitionLevel - 1  => Value does not exist, because one of its optional parent fields is null
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

    public static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        if (type instanceof RowType rowType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (RowType.Field rowField : fields) {
                String name = rowField.getName().orElseThrow().toLowerCase(Locale.ENGLISH);
                Optional<Field> field = constructField(rowField.getType(), lookupColumnByName(groupColumnIO, name));
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
            Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0));
            Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType arrayType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            Optional<Field> field = constructField(arrayType.getElementType(), getArrayElementColumn(groupColumnIO.getChild(0)));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        return Optional.of(new PrimitiveField(type, required, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId()));
    }
}
