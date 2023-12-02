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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.GroupField;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.assertj.core.util.Lists;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.writer.ParquetWriter.getBlockStarts;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestEncryption
{
    private final Configuration conf = new Configuration(false);

    @Test
    public void testBasicDecryption()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        Map<String, String> extraMetadata = new HashMap<String, String>()
        {
            {
                put("key1", "value1");
                put("key2", "value2");
            }
        };
        EncryptionTestFile inputFile = new EncryptionTestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("GZIP")
                .withExtraMeta(extraMetadata)
                .withPageSize(1000)
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testAllColumnsDecryption()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"id", "name", "gender"};
        EncryptionTestFile inputFile = new EncryptionTestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testNoColumnsDecryption()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {};
        EncryptionTestFile inputFile = new EncryptionTestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testOneRecord()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        EncryptionTestFile inputFile = new EncryptionTestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(1)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testMillionRows()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        EncryptionTestFile inputFile = new EncryptionTestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(1000000)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testPlainTextFooter()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        EncryptionTestFile inputFile = new EncryptionTestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("SNAPPY")
                .withPageSize(1000)
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testLargePageSize()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        EncryptionTestFile inputFile = new EncryptionTestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(100000)
                .withCodec("GZIP")
                .withPageSize(100000)
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testAesGcmCtr()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        EncryptionTestFile inputFile = new EncryptionTestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(100000)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withEncrytionAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .build();
        decryptAndValidate(inputFile);
    }

    private MessageType createSchema()
    {
        return new MessageType("schema",
                new PrimitiveType(OPTIONAL, INT64, "id"),
                new PrimitiveType(REQUIRED, BINARY, "name"),
                new PrimitiveType(OPTIONAL, BINARY, "gender"));
    }

    private void decryptAndValidate(EncryptionTestFile inputFile)
            throws IOException
    {
        Path path = new Path(inputFile.getFileName());
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        long fileSize = fileSystem.getFileStatus(path).getLen();
        Optional<InternalFileDecryptor> fileDecryptor = createFileDecryptor();
        ParquetDataSource dataSource = new MockParquetDataSource(new ParquetDataSourceId(path.toString()), fileSize, inputStream);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource,
//                inputFile.getFileSize(),
                Optional.empty(),
                fileDecryptor);
//                .getParquetMetadata();
        FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();
        MessageColumnIO messageColumn = getColumnIO(fileSchema, fileSchema);
        ParquetReader parquetReader = createParquetReader(parquetMetadata, messageColumn, dataSource, fileDecryptor);
        validateFile(parquetReader, messageColumn, inputFile);
    }

    private Optional<InternalFileDecryptor> createFileDecryptor()
    {
        FileDecryptionProperties fileDecryptionProperties = EncryptDecryptUtil.getFileDecryptionProperties();
        if (fileDecryptionProperties != null) {
            return Optional.of(new InternalFileDecryptor(fileDecryptionProperties));
        }
        return Optional.empty();
    }

    private ParquetReader createParquetReader(ParquetMetadata parquetMetadata,
            MessageColumnIO messageColumn,
            ParquetDataSource dataSource,
            Optional<InternalFileDecryptor> fileDecryptor)
    {
        ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
        ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();

        long nextStart = 0;
        for (BlockMetaData block : parquetMetadata.getBlocks()) {
            blocks.add(block);
            blockStarts.add(nextStart);
            nextStart += block.getRowCount();
        }

        List<Column> columns =
                ImmutableList.of(
                        constructColumn(BIGINT, lookupColumnByName(messageColumn, "id")).get(),
                        constructColumn(VARCHAR, lookupColumnByName(messageColumn, "name")).get(),
                        constructColumn(VARCHAR, lookupColumnByName(messageColumn, "gender")).get()
                ); // ColumnFields

        try {
            return new ParquetReader(
                    Optional.ofNullable(parquetMetadata.getFileMetaData().getCreatedBy()),
                    columns,
                    blocks.build(),
                    getBlockStarts(parquetMetadata),
                    dataSource,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    new ParquetReaderOptions(),
                    exception -> handleException(dataSource.getId(), exception),
                    Optional.empty(),
                    Lists.emptyList(),
                    Optional.empty(),
                    fileDecryptor);
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(() -> new ErrorCode(123, "wyu-code-1", ErrorType.INTERNAL_ERROR), exception);
        }
        return new TrinoException(() -> new ErrorCode(123, "wyu-code-2", ErrorType.INTERNAL_ERROR), format("Failed to read Parquet file: %s", dataSourceId), exception);
    }

    private void validateFile(ParquetReader parquetReader, MessageColumnIO messageColumn, EncryptionTestFile inputFile)
            throws IOException
    {
        int rowIndex = 0;
        // nb(wyu): original
//        int batchSize = parquetReader.nextBatch();
        io.trino.spi.Page page = parquetReader.nextPage();
        while (page != null) {
            validateColumn("id", BIGINT, rowIndex, parquetReader, messageColumn, inputFile);
            validateColumn("name", VARCHAR, rowIndex, parquetReader, messageColumn, inputFile);
            validateColumn("gender", VARCHAR, rowIndex, parquetReader, messageColumn, inputFile);
            rowIndex += page.getPositionCount();
            page = parquetReader.nextPage();
        }
    }

    private void validateColumn(String name, Type type, int rowIndex, ParquetReader parquetReader, MessageColumnIO messageColumn, EncryptionTestFile inputFile)
            throws IOException
    {
        Block block = parquetReader.readBlock(constructField(type, lookupColumnByName(messageColumn, name)).orElse(null));
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (type.equals(BIGINT)) {
                assertEquals(inputFile.getFileContent()[rowIndex++].getLong(name, 0), block.getLong(i, 0));
            }
            else if (type.equals(INT32)) {
                assertEquals(inputFile.getFileContent()[rowIndex++].getInteger(name, 0), block.getInt(i, 0));
            }
            else if (type.equals(VARCHAR)) {
                assertEquals(inputFile.getFileContent()[rowIndex++].getString(name, 0), block.getSlice(i, 0, block.getSliceLength(i)).toStringUtf8());
            }
        }
    }

    private Optional<Column> constructColumn(Type type, ColumnIO columnIO)
    {
        Optional<Field> field = constructField(type, columnIO);
        if (field.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Column(columnIO.getName(), field.get()));
    }

    private Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field rowField = fields.get(i);
                String name = rowField.getName().get().toLowerCase(Locale.ENGLISH);
                Optional<Field> field = constructField(rowField.getType(), lookupColumnByName(groupColumnIO, name));
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0));
            Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            Optional<Field> field = constructField(arrayType.getElementType(), getArrayElementColumn(groupColumnIO.getChild(0)));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        ColumnDescriptor column = new ColumnDescriptor(primitiveColumnIO.getColumnDescriptor().getPath(), columnIO.getType().asPrimitiveType(), repetitionLevel, definitionLevel);
        return Optional.of(new PrimitiveField(type, required, column, primitiveColumnIO.getId()));
    }
}
