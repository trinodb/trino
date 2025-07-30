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
package io.trino.hive.formats.line.sequence;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.testing.TempFile;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.line.AbstractTestLineReaderWriter;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.hive.formats.FormatTestUtils.COMPRESSION;
import static io.trino.hive.formats.FormatTestUtils.configureCompressionCodecs;
import static io.trino.hive.formats.ReadWriteUtils.findFirstSyncPosition;
import static io.trino.hive.formats.compression.CompressionKind.LZOP;
import static io.trino.hive.formats.compression.CompressionKind.ZSTD;
import static io.trino.hive.formats.line.sequence.SequenceFileWriter.TRINO_SEQUENCE_FILE_WRITER_VERSION;
import static io.trino.hive.formats.line.sequence.SequenceFileWriter.TRINO_SEQUENCE_FILE_WRITER_VERSION_METADATA_KEY;
import static io.trino.hive.formats.line.sequence.ValueType.BYTES;
import static io.trino.hive.formats.line.sequence.ValueType.TEXT;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSequenceFileReaderWriter
        extends AbstractTestLineReaderWriter
{
    @Test
    public void testLzopDisabled()
            throws Exception
    {
        for (boolean blockCompressed : ImmutableList.of(true)) {
            try (TempFile tempFile = new TempFile()) {
                assertThatThrownBy(() -> new SequenceFileWriter(new FileOutputStream(tempFile.file()), Optional.of(LZOP), blockCompressed, ImmutableMap.of(), TEXT))
                        .isInstanceOf(IllegalArgumentException.class);
            }

            try (TempFile tempFile = new TempFile()) {
                writeOld(tempFile.file(), Optional.of(LZOP), ImmutableList.of("test"), blockCompressed, Text::new, Text.class);
                assertThatThrownBy(() -> new SequenceFileReader(new LocalInputFile(tempFile.file()), 0, tempFile.file().length()))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @Override
    protected void testRoundTrip(List<String> values)
            throws Exception
    {
        testRoundTrip(values, Text::new, Text::new, Text.class, TEXT);
    }

    @Test
    public void testRoundTripBytesWritable()
            throws Exception
    {
        testRoundTrip(List.of("foo", "bar"), BytesWritable::new, value -> new BytesWritable(value.getBytes(UTF_8)), BytesWritable.class, BYTES);
    }

    protected <T extends Writable> void testRoundTrip(List<String> values, Supplier<T> newWritable, Function<String, T> newStringWritable, Class<T> clazz, ValueType valueType)
            throws Exception
    {
        for (Optional<CompressionKind> compressionKind : COMPRESSION) {
            if (compressionKind.equals(Optional.of(LZOP)) || compressionKind.equals(Optional.of(ZSTD))) {
                continue;
            }
            for (boolean blockCompressed : ImmutableList.of(true)) {
                // block compression is only allowed when compression is enabled
                if (compressionKind.isEmpty() && blockCompressed) {
                    continue;
                }

                // write old, read new and old
                try (TempFile tempFile = new TempFile()) {
                    writeOld(tempFile.file(), compressionKind, values, blockCompressed, newStringWritable, clazz);
                    assertOld(tempFile.file(), values, ImmutableMap.of(), compressionKind, blockCompressed, newWritable, clazz);
                    assertNew(tempFile.file(), values, ImmutableMap.of(), clazz);
                    assertOld(tempFile.file(), values, ImmutableMap.of(), compressionKind, blockCompressed, newWritable, clazz);
                }

                // write new, read old and new
                try (TempFile tempFile = new TempFile()) {
                    Map<String, String> metadata = ImmutableMap.of(
                            String.valueOf(ThreadLocalRandom.current().nextLong()),
                            String.valueOf(ThreadLocalRandom.current().nextLong()));
                    writeNew(tempFile.file(), values, metadata, compressionKind, blockCompressed, valueType);

                    Map<String, String> expectedMetadata = ImmutableMap.<String, String>builder()
                            .putAll(metadata)
                            .put(TRINO_SEQUENCE_FILE_WRITER_VERSION_METADATA_KEY, TRINO_SEQUENCE_FILE_WRITER_VERSION)
                            .buildOrThrow();

                    assertOld(tempFile.file(), values, expectedMetadata, compressionKind, blockCompressed, newWritable, clazz);
                    assertNew(tempFile.file(), values, expectedMetadata, clazz);
                }
            }
        }
    }

    private static <T extends Writable> void assertNew(File inputFile, List<String> values, Map<String, String> metadata, Class<T> clazz)
            throws IOException
    {
        LineBuffer lineBuffer = createLineBuffer(values);
        try (SequenceFileReader reader = createSequenceFileReader(inputFile)) {
            assertThat(reader.getFileLocation().toString()).isEqualTo(inputFile.toURI().toString());

            assertSyncPoint(reader, inputFile);
            assertThat(reader.getKeyClassName()).isEqualTo(BytesWritable.class.getName());
            assertThat(reader.getValueClassName()).isEqualTo(clazz.getName());
            assertThat(reader.getMetadata()).isEqualTo(metadata);

            for (String expected : values) {
                assertThat(reader.readLine(lineBuffer)).isTrue();
                String actual = new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8);
                assertThat(actual).isEqualTo(expected);
            }
            assertThat(reader.readLine(lineBuffer)).isFalse();
            assertThat(reader.getRowsRead()).isEqualTo(values.size());

            assertThat(reader.getReadTimeNanos()).isGreaterThan(0);
            assertThat(inputFile.length()).isEqualTo(reader.getBytesRead());
        }
    }

    private static void assertSyncPoint(SequenceFileReader reader, File file)
            throws IOException
    {
        List<Long> syncPositionsBruteForce = getSyncPositionsBruteForce(reader, file);
        List<Long> syncPositionsSimple = getSyncPositionsSimple(reader, file);

        assertThat(syncPositionsBruteForce).isEqualTo(syncPositionsSimple);
    }

    private static List<Long> getSyncPositionsBruteForce(SequenceFileReader reader, File file)
    {
        Slice slice = Slices.allocate(toIntExact(file.length()));
        try (InputStream in = new FileInputStream(file)) {
            slice.setBytes(0, in, slice.length());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        List<Long> syncPositionsBruteForce = new ArrayList<>();
        Slice sync = Slices.allocate(SIZE_OF_INT + SIZE_OF_LONG + SIZE_OF_LONG);
        sync.setInt(0, -1);
        sync.setBytes(SIZE_OF_INT, reader.getSync());

        long syncPosition = 0;
        while (syncPosition >= 0) {
            syncPosition = slice.indexOf(sync, toIntExact(syncPosition));
            if (syncPosition > 0) {
                syncPositionsBruteForce.add(syncPosition);
                syncPosition++;
            }
        }
        return syncPositionsBruteForce;
    }

    private static List<Long> getSyncPositionsSimple(SequenceFileReader recordReader, File file)
            throws IOException
    {
        List<Long> syncPositions = new ArrayList<>();
        Slice sync = recordReader.getSync();
        long syncFirst = sync.getLong(0);
        long syncSecond = sync.getLong(8);
        long syncPosition = 0;
        TrinoInputFile inputFile = new LocalInputFile(file);
        while (syncPosition >= 0) {
            syncPosition = findFirstSyncPosition(inputFile, syncPosition, file.length() - syncPosition, syncFirst, syncSecond);
            if (syncPosition > 0) {
                assertThat(findFirstSyncPosition(inputFile, syncPosition, 1, syncFirst, syncSecond)).isEqualTo(syncPosition);
                assertThat(findFirstSyncPosition(inputFile, syncPosition, 2, syncFirst, syncSecond)).isEqualTo(syncPosition);
                assertThat(findFirstSyncPosition(inputFile, syncPosition, 10, syncFirst, syncSecond)).isEqualTo(syncPosition);

                assertThat(findFirstSyncPosition(inputFile, syncPosition - 1, 1, syncFirst, syncSecond)).isEqualTo(-1);
                assertThat(findFirstSyncPosition(inputFile, syncPosition - 2, 2, syncFirst, syncSecond)).isEqualTo(-1);
                assertThat(findFirstSyncPosition(inputFile, syncPosition + 1, 1, syncFirst, syncSecond)).isEqualTo(-1);

                syncPositions.add(syncPosition);
                syncPosition++;
            }
        }
        return syncPositions;
    }

    private static SequenceFileReader createSequenceFileReader(File inputFile)
            throws IOException
    {
        return new SequenceFileReader(
                new LocalInputFile(inputFile),
                0,
                inputFile.length());
    }

    private static void writeNew(File outputFile, List<String> values, Map<String, String> metadata, Optional<CompressionKind> compressionKind, boolean blockCompressed, ValueType valueType)
            throws Exception
    {
        try (LineWriter writer = new SequenceFileWriter(
                new FileOutputStream(outputFile),
                compressionKind,
                blockCompressed,
                metadata,
                valueType)) {
            for (String value : values) {
                writer.write(Slices.utf8Slice(value));
            }
            assertThat(writer.getRetainedSizeInBytes()).isGreaterThan(0);
        }
    }

    private static <T extends Writable> void assertOld(File inputFile, List<String> values, Map<String, String> metadata, Optional<CompressionKind> compressionKind, boolean blockCompressed, Supplier<T> newWritable, Class<T> clazz)
            throws IOException
    {
        JobConf jobConf = new JobConf(false);
        configureCompressionCodecs(jobConf);
        Path path = new Path(inputFile.toURI());
        try (SequenceFile.Reader reader = new SequenceFile.Reader(jobConf, SequenceFile.Reader.file(path))) {
            assertThat(reader.getKeyClassName()).isEqualTo(BytesWritable.class.getName());
            assertThat(reader.getValueClassName()).isEqualTo(clazz.getName());

            Map<String, String> actualMetadata = reader.getMetadata().getMetadata().entrySet().stream()
                    .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().toString()));
            assertThat(actualMetadata).isEqualTo(metadata);

            switch (reader.getCompressionType()) {
                case NONE -> assertThat(compressionKind).isEmpty();
                case RECORD -> {
                    assertThat(compressionKind).isPresent();
                    assertThat(blockCompressed).isFalse();
                }
                case BLOCK -> {
                    assertThat(compressionKind).isPresent();
                    assertThat(blockCompressed).isTrue();
                }
            }

            BytesWritable key = new BytesWritable();
            T value = newWritable.get();
            for (String expected : values) {
                assertThat(reader.next(key, value)).isTrue();

                BinaryComparable binaryComparable = (BinaryComparable) value;
                byte[] buffer = new byte[binaryComparable.getLength()];
                System.arraycopy(binaryComparable.getBytes(), 0, buffer, 0, binaryComparable.getLength());
                assertThat(buffer).isEqualTo(expected.getBytes(UTF_8));
            }

            assertThat(reader.next(key, value)).isFalse();
        }
    }

    private static <T extends Writable> void writeOld(File outputFile, Optional<CompressionKind> compressionKind, List<String> values, boolean blockCompressed, Function<String, T> newStringWritable, Class<T> clazz)
            throws Exception
    {
        RecordWriter recordWriter = createWriterOld(outputFile, compressionKind, blockCompressed, clazz);

        for (String value : values) {
            recordWriter.write(newStringWritable.apply(value));
        }

        recordWriter.close(false);
    }

    private static <T extends Writable> RecordWriter createWriterOld(File outputFile, Optional<CompressionKind> compressionKind, boolean blockCompressed, Class<T> clazz)
            throws IOException
    {
        JobConf jobConf = new JobConf(false);
        configureCompressionCodecs(jobConf);
        compressionKind.ifPresent(kind -> {
            jobConf.set(COMPRESS_CODEC, kind.getHadoopClassName());
            jobConf.set(COMPRESS_TYPE, blockCompressed ? "BLOCK" : "RECORD");
        });

        return new HiveSequenceFileOutputFormat<>().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                clazz,
                compressionKind.isPresent(),
                new Properties(),
                () -> {});
    }
}
