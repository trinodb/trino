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

package io.trino.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.local.LocalOutputFile;
import io.trino.orc.metadata.OrcType;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.trino.orc.TestingOrcPredicate.ORC_ROW_GROUP_SIZE;
import static io.trino.orc.TestingOrcPredicate.ORC_STRIPE_SIZE;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestStructColumnReader
{
    private static final String STRUCT_COL_NAME = "struct_col";

    private TempFile tempFile;

    @BeforeEach
    public void setUp()
            throws IOException
    {
        tempFile = new TempFile();
    }

    @AfterEach
    public void tearDown()
            throws IOException
    {
        tempFile.close();
        tempFile = null;
    }

    /**
     * Reader and writer have the same fields. Checks that fields are read in correctly
     */
    @Test
    public void testValuesAreReadInCorrectly()
            throws IOException
    {
        List<String> readerFields = new ArrayList<>(Arrays.asList("field_a", "field_b", "field_c"));
        List<String> writerFields = new ArrayList<>(Arrays.asList("field_a", "field_b", "field_c"));
        List<String> writerData = new ArrayList<>(Arrays.asList("field_a_value", "field_b_value", "field_c_value"));
        Type readerType = getType(readerFields);
        Type writerType = getType(writerFields);

        write(tempFile, writerType, writerData);
        RowBlock readBlock = read(tempFile, readerType);
        List<?> actual = (List<?>) readerType.getObjectValue(TestingConnectorSession.SESSION, readBlock, 0);

        assertThat(actual).hasSize(readerFields.size());
        assertThat(actual.get(0)).isEqualTo("field_a_value");
        assertThat(actual.get(1)).isEqualTo("field_b_value");
        assertThat(actual.get(2)).isEqualTo("field_c_value");
    }

    /**
     * The writer has fields with upper case characters, reader has same names downcased.
     */
    @Test
    public void testReaderLowerCasesFieldNamesFromStream()
            throws IOException
    {
        List<String> readerFields = new ArrayList<>(Arrays.asList("field_a", "field_b", "field_c"));
        List<String> writerFields = new ArrayList<>(Arrays.asList("field_A", "field_B", "field_C"));
        List<String> writerData = new ArrayList<>(Arrays.asList("fieldAValue", "fieldBValue", "fieldCValue"));
        Type readerType = getType(readerFields);
        Type writerType = getType(writerFields);

        write(tempFile, writerType, writerData);
        RowBlock readBlock = read(tempFile, readerType);
        List<?> actual = (List<?>) readerType.getObjectValue(TestingConnectorSession.SESSION, readBlock, 0);

        assertThat(actual).hasSize(readerFields.size());
        assertThat(actual.get(0)).isEqualTo("fieldAValue");
        assertThat(actual.get(1)).isEqualTo("fieldBValue");
        assertThat(actual.get(2)).isEqualTo("fieldCValue");
    }

    /**
     * Reader has fields with upper case characters, writer has same names downcased.
     */
    @Test
    public void testReaderLowerCasesFieldNamesFromType()
            throws IOException
    {
        List<String> readerFields = new ArrayList<>(Arrays.asList("field_A", "field_B", "field_C"));
        List<String> writerFields = new ArrayList<>(Arrays.asList("field_a", "field_b", "field_c"));
        List<String> writerData = new ArrayList<>(Arrays.asList("fieldAValue", "fieldBValue", "fieldCValue"));
        Type readerType = getType(readerFields);
        Type writerType = getType(writerFields);

        write(tempFile, writerType, writerData);
        RowBlock readBlock = read(tempFile, readerType);
        List<?> actual = (List<?>) readerType.getObjectValue(TestingConnectorSession.SESSION, readBlock, 0);

        assertThat(actual).hasSize(readerFields.size());
        assertThat(actual.get(0)).isEqualTo("fieldAValue");
        assertThat(actual.get(1)).isEqualTo("fieldBValue");
        assertThat(actual.get(2)).isEqualTo("fieldCValue");
    }

    @Test
    public void testThrowsExceptionWhenFieldNameMissing()
    {
        assertThatThrownBy(() -> {
            List<String> readerFields = new ArrayList<>(Arrays.asList("field_a", "field_b", "field_c"));
            List<String> writerFields = new ArrayList<>(Arrays.asList("field_a", "field_b", "field_c"));
            List<String> writerData = new ArrayList<>(Arrays.asList("field_a_value", "field_b_value", "field_c_value"));
            Type readerType = getTypeNullName(readerFields.size());
            Type writerType = getType(writerFields);

            write(tempFile, writerType, writerData);
            read(tempFile, readerType);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ROW type does not have field names declared: row(varchar, varchar, varchar)");
    }

    /**
     * The reader has a field that is missing from the ORC file
     */
    @Test
    public void testExtraFieldsInReader()
            throws IOException
    {
        List<String> readerFields = new ArrayList<>(Arrays.asList("field_a", "field_b", "field_c"));

        // field_b is missing
        List<String> writerFields = new ArrayList<>(Arrays.asList("field_a", "field_c"));
        List<String> writerData = new ArrayList<>(Arrays.asList("field_a_value", "field_c_value"));
        Type readerType = getType(readerFields);
        Type writerType = getType(writerFields);

        write(tempFile, writerType, writerData);
        RowBlock readBlock = read(tempFile, readerType);
        List<?> actual = (List<?>) readerType.getObjectValue(TestingConnectorSession.SESSION, readBlock, 0);

        assertThat(actual).hasSize(readerFields.size());
        assertThat(actual.get(0)).isEqualTo("field_a_value");
        assertThat(actual.get(1)).isNull();
        assertThat(actual.get(2)).isEqualTo("field_c_value");
    }

    /**
     * The ORC file has a field that is missing from the reader
     */
    @Test
    public void testExtraFieldsInWriter()
            throws IOException
    {
        // field_b is missing
        List<String> readerFields = new ArrayList<>(Arrays.asList("field_a", "field_c"));
        List<String> writerFields = new ArrayList<>(Arrays.asList("field_a", "field_b", "field_c"));
        List<String> writerData = new ArrayList<>(Arrays.asList("field_a_value", "field_b_value", "field_c_value"));
        Type readerType = getType(readerFields);
        Type writerType = getType(writerFields);

        write(tempFile, writerType, writerData);
        RowBlock readBlock = read(tempFile, readerType);
        List<?> actual = (List<?>) readerType.getObjectValue(TestingConnectorSession.SESSION, readBlock, 0);

        assertThat(actual).hasSize(readerFields.size());
        assertThat(actual.get(0)).isEqualTo("field_a_value");
        assertThat(actual.get(1)).isEqualTo("field_c_value");
    }

    private void write(TempFile tempFile, Type writerType, List<String> data)
            throws IOException
    {
        List<String> columnNames = ImmutableList.of(STRUCT_COL_NAME);
        List<Type> types = ImmutableList.of(writerType);
        OrcWriter writer = new OrcWriter(
                OutputStreamOrcDataSink.create(new LocalOutputFile(tempFile.getFile())),
                columnNames,
                types,
                OrcType.createRootOrcType(columnNames, types),
                NONE,
                new OrcWriterOptions()
                        .withStripeMinSize(DataSize.of(0, MEGABYTE))
                        .withStripeMaxSize(DataSize.of(32, MEGABYTE))
                        .withStripeMaxRowCount(ORC_STRIPE_SIZE)
                        .withRowGroupMaxRowCount(ORC_ROW_GROUP_SIZE)
                        .withDictionaryMaxMemory(DataSize.of(32, MEGABYTE)),
                ImmutableMap.of(),
                true,
                BOTH,
                new OrcWriterStats());

        // write down some data with unsorted streams
        Block[] fieldBlocks = new Block[data.size()];

        int entries = 10;

        VariableWidthBlockBuilder fieldBlockBuilder = VARCHAR.createBlockBuilder(null, entries);
        for (int fieldId = 0; fieldId < data.size(); fieldId++) {
            Slice fieldValue = Slices.utf8Slice(data.get(fieldId));
            for (int rowId = 0; rowId < entries; rowId++) {
                fieldBlockBuilder.writeEntry(fieldValue);
            }
            fieldBlocks[fieldId] = fieldBlockBuilder.build();
            fieldBlockBuilder = (VariableWidthBlockBuilder) fieldBlockBuilder.newBlockBuilderLike(null);
        }
        Block rowBlock = RowBlock.fromFieldBlocks(entries, fieldBlocks);
        writer.write(new Page(rowBlock));
        writer.close();
    }

    private RowBlock read(TempFile tempFile, Type readerType)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
        OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                .orElseThrow(() -> new RuntimeException("File is empty"));

        OrcRecordReader recordReader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(readerType),
                false,
                OrcPredicate.TRUE,
                UTC,
                newSimpleAggregatedMemoryContext(),
                OrcReader.INITIAL_BATCH_SIZE,
                RuntimeException::new);

        RowBlock block = (RowBlock) recordReader.nextPage().getBlock(0).getLoadedBlock();
        recordReader.close();
        return block;
    }

    private Type getType(List<String> fieldNames)
    {
        ImmutableList.Builder<TypeSignatureParameter> typeSignatureParameters = ImmutableList.builder();
        for (String fieldName : fieldNames) {
            typeSignatureParameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName(fieldName)), VARCHAR.getTypeSignature())));
        }
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ROW, typeSignatureParameters.build());
    }

    private Type getTypeNullName(int numFields)
    {
        ImmutableList.Builder<TypeSignatureParameter> typeSignatureParameters = ImmutableList.builder();

        for (int i = 0; i < numFields; i++) {
            typeSignatureParameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.empty(), VARCHAR.getTypeSignature())));
        }
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ROW, typeSignatureParameters.build());
    }
}
