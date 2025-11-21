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
package io.trino.lance.file;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.file.LanceFileWriter;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestingLanceUtil
{
    public static final int MAX_LIST_SIZE = 256;

    private static final Random random = new Random();

    private TestingLanceUtil() {}

    public static void writeLanceColumnJNI(File outputFile, Type type, List<?> values, boolean nullable)
            throws Exception
    {
        BufferAllocator allocator = new RootAllocator();
        LanceFileWriter writer = LanceFileWriter.open(outputFile.getPath(), allocator, null, Optional.of(WriteParams.LanceFileVersion.V2_1), ImmutableMap.of());
        String columnName = type.getDisplayName();
        Field field = toArrowField(columnName, type, nullable);
        Schema schema = new Schema(ImmutableList.of(field), null);
        List<List<?>> data = values.stream().map(Arrays::asList).collect(toImmutableList());
        VectorSchemaRoot root = writeVectorSchemaRoot(schema, data, allocator);
        writer.write(root);
        writer.close();
    }

    private static ArrowType toArrowPrimitiveType(Type type)
    {
        return switch (type) {
            case TinyintType _ -> new ArrowType.Int(8, true);
            case SmallintType _ -> new ArrowType.Int(16, true);
            case IntegerType _ -> new ArrowType.Int(32, true);
            case BigintType _ -> new ArrowType.Int(64, true);
            case RealType _ -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DoubleType _ -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case VarcharType _ -> new ArrowType.Utf8();
            case VarbinaryType _ -> new ArrowType.Binary();
            default -> throw new UnsupportedOperationException();
        };
    }

    public static Field toArrowField(String name, Type type, boolean nullable)
    {
        return switch (type) {
            case TinyintType _, SmallintType _, IntegerType _, BigintType _, RealType _, DoubleType _, VarcharType _, VarbinaryType _ ->
                    new Field(name, nullable ? FieldType.nullable(toArrowPrimitiveType(type)) : FieldType.notNullable(toArrowPrimitiveType(type)), ImmutableList.of());
            case RowType row -> {
                List<Field> childFields = row.getFields().stream().map(child -> toArrowField(child.getName().orElse(""), child.getType(), nullable)).collect(toImmutableList());
                yield new Field(name, nullable ? FieldType.nullable(ArrowType.Struct.INSTANCE) : FieldType.notNullable(ArrowType.Struct.INSTANCE), childFields);
            }
            case ArrayType array -> {
                Field element = toArrowField("element", array.getElementType(), true);
                yield new Field(name, nullable ? FieldType.nullable(ArrowType.List.INSTANCE) : FieldType.notNullable(ArrowType.List.INSTANCE), ImmutableList.of(element));
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    public static void testRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        testRoundTripType(type, true, insertNullEvery(5, readValues));
        testRoundTripType(type, false, readValues);
        testSimpleStructRoundTrip(type, readValues);
        testSimpleListRoundTrip(type, readValues);
    }

    public static void testRoundTripType(Type type, boolean nullable, List<?> readValues)
            throws Exception
    {
        // For non-nullable tests, filter out null values
        List<?> filteredValues = nullable ? readValues : readValues.stream().filter(Objects::nonNull).collect(toList());
        assertRoundTrip(type, type, filteredValues, filteredValues, nullable);
    }

    private static void testSimpleListRoundTrip(Type type, List<?> values)
            throws Exception
    {
        Type arrayType = arrayType(type);
        List<?> data = values.stream().filter(Objects::nonNull).map(value -> insertNullEvery(9, nCopies(random.nextInt(MAX_LIST_SIZE), value))).collect(toImmutableList());
        testRoundTripType(arrayType, false, data);
        testRoundTripType(arrayType, true, insertNullEvery(7, data));
    }

    public static void testLongListRoundTrip(Type type, List<?> values)
            throws Exception
    {
        Type arrayType = arrayType(type);
        List<?> data = values.stream().filter(Objects::nonNull).map(value -> insertNullEvery(9, nCopies(random.nextInt(2048, 10000), value))).collect(toImmutableList());
        testRoundTripType(arrayType, false, data);
        testRoundTripType(arrayType, true, insertNullEvery(7, data));
    }

    private static void testSimpleStructRoundTrip(Type type, List<?> values)
            throws Exception
    {
        Type rowType = rowType(type, type, type);
        testRoundTripType(rowType, false, values.stream().map(value -> List.of(value, value, value)).collect(toList()));
        testRoundTripType(rowType, true, insertNullEvery(7, values.stream().map(value -> List.of(value, value, value)).collect(toList())));
    }

    private static <T> List<T> insertNullEvery(int n, List<T> iterable)
    {
        return newArrayList(() -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                position++;
                if (position > n) {
                    position = 0;
                    return null;
                }

                if (!delegate.hasNext()) {
                    return endOfData();
                }

                return delegate.next();
            }
        });
    }

    private static void assertRoundTrip(Type writeType, Type readType, List<?> writeValues, List<?> readValues, boolean nullable)
            throws Exception
    {
        // write w/ JNI writer, read w/ LanceReader
        try (TempFile file = new TempFile()) {
            writeLanceColumnJNI(file.getFile(), writeType, writeValues, nullable);
            assertFileContentsTrino(readType, file, readValues);
        }
    }

    private static void assertFileContentsTrino(Type type, TempFile tempFile, List<?> expectedValues)
            throws IOException
    {
        try (LanceDataSource dataSource = new FileLanceDataSource(tempFile.getFile());
                LanceReader lanceReader = new LanceReader(dataSource, ImmutableList.of(0), Optional.empty(), newSimpleAggregatedMemoryContext())) {
            Iterator<?> iterator = expectedValues.iterator();

            int rowsProcessed = 0;
            for (SourcePage page = lanceReader.nextSourcePage(); page != null; page = lanceReader.nextSourcePage()) {
                int batchSize = page.getPositionCount();
                Block block = page.getBlock(0);
                List<Object> data = new ArrayList<>(block.getPositionCount());
                for (int position = 0; position < block.getPositionCount(); position++) {
                    data.add(type.getObjectValue(block, position));
                }
                for (int i = 0; i < batchSize; i++) {
                    assertThat(iterator.hasNext()).isTrue();
                    Object expected = iterator.next();
                    Object actual = data.get(i);
                    assertColumnValueEquals(type, actual, expected);
                }
                rowsProcessed += batchSize;
            }
            assertThat(iterator.hasNext()).isFalse();
            assertThat(lanceReader.nextSourcePage()).isNull();
            assertThat(rowsProcessed).isEqualTo(expectedValues.size());
        }
    }

    private static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (expected == null) {
            assertThat(actual).isNull();
            return;
        }

        if (type instanceof RowType rowType) {
            List<Type> fieldTypes = rowType.getFieldTypes();
            List<?> actualRow = (List<?>) actual;
            List<?> expectedRow = (List<?>) expected;
            assertThat(actualRow).hasSize(fieldTypes.size());
            assertThat(actualRow).hasSize(expectedRow.size());
            for (int fieldId = 0; fieldId < actualRow.size(); fieldId++) {
                Type fieldType = fieldTypes.get(fieldId);
                Object actualElement = actualRow.get(fieldId);
                Object expectedElement = expectedRow.get(fieldId);
                assertColumnValueEquals(fieldType, actualElement, expectedElement);
            }
        }
        else if (!Objects.equals(actual, expected)) {
            assertThat(actual).isEqualTo(expected);
        }
    }

    public static Type arrayType(Type elementType)
    {
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeParameter.typeParameter(elementType.getTypeSignature())));
    }

    public static Type rowType(Type... fieldTypes)
    {
        ImmutableList.Builder<TypeParameter> typeParameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            String fieldName = "field_" + i;
            Type fieldType = fieldTypes[i];
            typeParameters.add(TypeParameter.typeParameter(Optional.of(fieldName), fieldType.getTypeSignature()));
        }
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ROW, typeParameters.build());
    }

    public static VectorSchemaRoot writeVectorSchemaRoot(Schema schema, List<List<?>> data, BufferAllocator allocator)
    {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();

        int rowCount = data.size();
        List<Field> fields = schema.getFields();

        for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            Field field = fields.get(fieldIndex);
            FieldVector vector = root.getVector(field.getName());

            List<Object> columnData = new ArrayList<>();
            for (List<?> row : data) {
                columnData.add(row.get(fieldIndex));
            }

            writeFieldVector(vector, field, columnData);
        }

        root.setRowCount(rowCount);
        return root;
    }

    private static void writeFieldVector(FieldVector vector, Field field, List<?> data)
    {
        ArrowType arrowType = field.getType();

        switch (arrowType) {
            case ArrowType.Int intType -> writeIntVector(vector, data, intType);
            case ArrowType.FloatingPoint floatingPointType -> writeFloatingPointVector(vector, data, floatingPointType);
            case ArrowType.Utf8 _ -> writeStringVector((VarCharVector) vector, data);
            case ArrowType.Binary _ -> writeBinaryVector((VarBinaryVector) vector, data);
            case ArrowType.Struct _ -> writeStructVector((StructVector) vector, field, data);
            case ArrowType.List _ -> writeListVector((ListVector) vector, field, data);
            case null, default -> throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
        }

        vector.setValueCount(data.size());
    }

    private static void writeIntVector(FieldVector vector, List<?> data, ArrowType.Int intType)
    {
        int bitWidth = intType.getBitWidth();

        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                Number num = (Number) value;
                switch (bitWidth) {
                    case 8 -> ((TinyIntVector) vector).setSafe(i, num.byteValue());
                    case 16 -> ((SmallIntVector) vector).setSafe(i, num.shortValue());
                    case 32 -> ((IntVector) vector).setSafe(i, num.intValue());
                    case 64 -> ((BigIntVector) vector).setSafe(i, num.longValue());
                    default -> throw new UnsupportedOperationException("Unsupported bit width: " + bitWidth);
                }
            }
        }
    }

    private static void writeFloatingPointVector(FieldVector vector, List<?> data, ArrowType.FloatingPoint floatType)
    {
        FloatingPointPrecision precision = floatType.getPrecision();

        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                Number num = (Number) value;
                switch (precision) {
                    case SINGLE -> ((Float4Vector) vector).setSafe(i, num.floatValue());
                    case DOUBLE -> ((Float8Vector) vector).setSafe(i, num.doubleValue());
                    default -> throw new UnsupportedOperationException("Unsupported precision: " + precision);
                }
            }
        }
    }

    private static void writeStringVector(VarCharVector vector, List<?> data)
    {
        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                String str = value.toString();
                vector.setSafe(i, str.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static void writeBinaryVector(VarBinaryVector vector, List<?> data)
    {
        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                byte[] bytes = (byte[]) value;
                vector.setSafe(i, bytes);
            }
        }
    }

    private static void writeStructVector(StructVector vector, Field field, List<?> data)
    {
        List<Field> childFields = field.getChildren();

        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                List<?> structData = (List<?>) value;
                vector.setIndexDefined(i);

                // Populate child vectors
                for (int childIndex = 0; childIndex < childFields.size(); childIndex++) {
                    Field childField = childFields.get(childIndex);
                    FieldVector childVector = vector.getChild(childField.getName());
                    Object childValue = structData.get(childIndex);

                    if (childValue == null) {
                        childVector.setNull(i);
                    }
                    else {
                        // For primitive types in struct, we need to handle them individually
                        writeStructChildVector(childVector, childField, i, childValue);
                    }
                }
            }
        }

        // Set value count for child vectors
        for (Field childField : childFields) {
            FieldVector childVector = vector.getChild(childField.getName());
            childVector.setValueCount(data.size());
        }
    }

    private static void writeStructChildVector(FieldVector childVector, Field childField, int index, Object value)
    {
        ArrowType arrowType = childField.getType();

        switch (arrowType) {
            case ArrowType.Int intType -> {
                Number num = (Number) value;
                switch (intType.getBitWidth()) {
                    case 8 -> ((TinyIntVector) childVector).setSafe(index, num.byteValue());
                    case 16 -> ((SmallIntVector) childVector).setSafe(index, num.shortValue());
                    case 32 -> ((IntVector) childVector).setSafe(index, num.intValue());
                    case 64 -> ((BigIntVector) childVector).setSafe(index, num.longValue());
                    default -> throw new UnsupportedOperationException("Unsupported bit width: " + intType.getBitWidth());
                }
            }
            case ArrowType.FloatingPoint floatType -> {
                Number num = (Number) value;
                switch (floatType.getPrecision()) {
                    case SINGLE -> ((Float4Vector) childVector).setSafe(index, num.floatValue());
                    case DOUBLE -> ((Float8Vector) childVector).setSafe(index, num.doubleValue());
                    default -> throw new UnsupportedOperationException("Unsupported precision: " + floatType.getPrecision());
                }
            }
            case ArrowType.Utf8 _ -> {
                String str = value.toString();
                ((VarCharVector) childVector).setSafe(index, str.getBytes(StandardCharsets.UTF_8));
            }
            case ArrowType.Binary _ -> {
                byte[] bytes = (byte[]) value;
                ((VarBinaryVector) childVector).setSafe(index, bytes);
            }
            case null, default -> throw new UnsupportedOperationException("Unsupported ArrowType " + arrowType + " in struct field");
        }
    }

    public static void writeListVector(ListVector vector, Field field, List<?> data)
    {
        checkArgument(field.getChildren().size() == 1, "List field must have a single child");
        Field elementField = field.getChildren().getFirst();
        vector.setInitialCapacity(data.size());
        UnionListWriter writer = vector.getWriter();
        for (int i = 0; i < data.size(); i++) {
            Object list = data.get(i);
            writer.setPosition(i);
            if (list == null) {
                writer.writeNull();
                continue;
            }
            writer.startList();
            for (Object value : (List<?>) list) {
                if (value == null) {
                    if (!elementField.isNullable()) {
                        throw new IllegalArgumentException("Cannot write null value to non-nullable array element field: " + elementField.getName());
                    }
                    writer.writeNull();
                }
                else {
                    switch (elementField.getType()) {
                        case ArrowType.Int intType -> {
                            switch (intType.getBitWidth()) {
                                case 8 -> writer.writeTinyInt((byte) value);
                                case 16 -> writer.writeSmallInt((short) value);
                                case 32 -> writer.writeInt((int) value);
                                case 64 -> writer.writeBigInt((long) value);
                            }
                        }
                        case ArrowType.FloatingPoint floatType -> {
                            switch (floatType.getPrecision()) {
                                case SINGLE -> writer.writeFloat4((float) value);
                                case DOUBLE -> writer.writeFloat8((double) value);
                                default -> throw new UnsupportedOperationException("Unsupported floatType " + floatType.getPrecision());
                            }
                        }
                        case ArrowType.Utf8 _ -> writer.writeVarChar((String) value);
                        case ArrowType.Binary _ -> writer.writeVarBinary((byte[]) value);
                        default -> throw new IllegalStateException("Unexpected value: " + elementField.getFieldType());
                    }
                }
            }
            writer.endList();
        }
        writer.setValueCount(data.size());
        vector.setValueCount(data.size());
    }
}
