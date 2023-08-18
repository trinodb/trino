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
package io.trino.parquet.writer;

import io.trino.parquet.writer.valuewriter.DictionaryFallbackValuesWriter;
import io.trino.parquet.writer.valuewriter.TrinoValuesWriterFactory;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.testng.annotations.Test;

import static java.util.Locale.ENGLISH;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.schema.Types.required;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoValuesWriterFactory
{
    @Test
    public void testBoolean()
    {
        testValueWriter(PrimitiveTypeName.BOOLEAN, BooleanPlainValuesWriter.class);
    }

    @Test
    public void testFixedLenByteArray()
    {
        testValueWriter(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, FixedLenByteArrayPlainValuesWriter.class);
    }

    @Test
    public void testBinary()
    {
        testValueWriter(
                PrimitiveTypeName.BINARY,
                PlainBinaryDictionaryValuesWriter.class,
                PlainValuesWriter.class);
    }

    @Test
    public void testInt32()
    {
        testValueWriter(
                PrimitiveTypeName.INT32,
                PlainIntegerDictionaryValuesWriter.class,
                PlainValuesWriter.class);
    }

    @Test
    public void testInt64()
    {
        testValueWriter(
                PrimitiveTypeName.INT64,
                PlainLongDictionaryValuesWriter.class,
                PlainValuesWriter.class);
    }

    @Test
    public void testInt96()
    {
        testValueWriter(
                PrimitiveTypeName.INT96,
                PlainFixedLenArrayDictionaryValuesWriter.class,
                FixedLenByteArrayPlainValuesWriter.class);
    }

    @Test
    public void testDouble()
    {
        testValueWriter(
                PrimitiveTypeName.DOUBLE,
                PlainDoubleDictionaryValuesWriter.class,
                PlainValuesWriter.class);
    }

    @Test
    public void testFloat()
    {
        testValueWriter(
                PrimitiveTypeName.FLOAT,
                PlainFloatDictionaryValuesWriter.class,
                PlainValuesWriter.class);
    }

    private void testValueWriter(PrimitiveTypeName typeName, Class<? extends ValuesWriter> expectedValueWriterClass)
    {
        ColumnDescriptor mockPath = createColumnDescriptor(typeName);
        TrinoValuesWriterFactory factory = new TrinoValuesWriterFactory(ParquetProperties.builder()
                .withWriterVersion(PARQUET_1_0)
                .build());
        ValuesWriter writer = factory.newValuesWriter(mockPath);

        validateWriterType(writer, expectedValueWriterClass);
    }

    private void testValueWriter(PrimitiveTypeName typeName, Class<? extends ValuesWriter> initialValueWriterClass, Class<? extends ValuesWriter> fallbackValueWriterClass)
    {
        ColumnDescriptor mockPath = createColumnDescriptor(typeName);
        TrinoValuesWriterFactory factory = new TrinoValuesWriterFactory(ParquetProperties.builder()
                .withWriterVersion(PARQUET_1_0)
                .build());
        ValuesWriter writer = factory.newValuesWriter(mockPath);

        validateFallbackWriter(writer, initialValueWriterClass, fallbackValueWriterClass);
    }

    private ColumnDescriptor createColumnDescriptor(PrimitiveTypeName typeName)
    {
        return createColumnDescriptor(typeName, "fake_" + typeName.name().toLowerCase(ENGLISH) + "_col");
    }

    private ColumnDescriptor createColumnDescriptor(PrimitiveTypeName typeName, String name)
    {
        return new ColumnDescriptor(new String[] {name}, required(typeName).length(1).named(name), 0, 0);
    }

    private void validateWriterType(ValuesWriter writer, Class<? extends ValuesWriter> valuesWriterClass)
    {
        assertThat(writer).isInstanceOf(valuesWriterClass);
    }

    private void validateFallbackWriter(ValuesWriter writer, Class<? extends ValuesWriter> initialWriterClass, Class<? extends ValuesWriter> fallbackWriterClass)
    {
        validateWriterType(writer, DictionaryFallbackValuesWriter.class);

        DictionaryFallbackValuesWriter fallbackValuesWriter = (DictionaryFallbackValuesWriter) writer;
        validateWriterType(fallbackValuesWriter.getInitialWriter(), initialWriterClass);
        validateWriterType(fallbackValuesWriter.getFallBackWriter(), fallbackWriterClass);
    }
}
