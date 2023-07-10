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
package io.trino.hive.formats.line.text;

import io.airlift.slice.Slices;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.line.AbstractTestLineReaderWriter;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.FormatTestUtils.COMPRESSION;
import static io.trino.hive.formats.FormatTestUtils.configureCompressionCodecs;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTextLineReaderWriter
        extends AbstractTestLineReaderWriter
{
    @Override
    protected void testRoundTrip(List<String> values)
            throws Exception
    {
        for (Optional<CompressionKind> compressionKind : COMPRESSION) {
            // write old, read new and ol
            try (TempFileWithExtension tempFile = new TempFileWithExtension(compressionKind.map(CompressionKind::getFileExtension).orElse(""))) {
                writeOld(tempFile.file(), compressionKind, values);
                assertNew(tempFile, values, compressionKind);
                assertOld(tempFile.file(), values);
            }

            // write new, read old and new
            try (TempFileWithExtension tempFile = new TempFileWithExtension(compressionKind.map(CompressionKind::getFileExtension).orElse(""))) {
                writeNew(tempFile.file(), values, compressionKind);
                assertOld(tempFile.file(), values);
                assertNew(tempFile, values, compressionKind);
            }
        }
    }

    private static void assertNew(TempFileWithExtension tempFile, List<String> values, Optional<CompressionKind> compressionKind)
            throws IOException
    {
        LineBuffer lineBuffer = createLineBuffer(values);
        try (TextLineReader reader = createTextLineReader(tempFile, compressionKind)) {
            int linesRead = 0;
            for (String expected : values) {
                assertTrue(reader.readLine(lineBuffer));
                String actual = new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8);
                assertEquals(actual, expected);
                linesRead++;
            }
            assertFalse(reader.readLine(lineBuffer));
            assertEquals(linesRead, values.size());

            assertThat(reader.getReadTimeNanos()).isGreaterThan(0);
            assertThat(reader.getBytesRead()).isGreaterThan(0);
        }
    }

    private static TextLineReader createTextLineReader(TempFileWithExtension tempFile, Optional<CompressionKind> compressionKind)
            throws IOException
    {
        InputStream inputStream = new FileInputStream(tempFile.file());
        if (compressionKind.isPresent()) {
            inputStream = compressionKind.get().createCodec().createStreamDecompressor(inputStream);
        }
        return new TextLineReader(inputStream, 1024);
    }

    private static void writeNew(File outputFile, List<String> values, Optional<CompressionKind> compressionKind)
            throws Exception
    {
        try (OutputStream outputStream = new FileOutputStream(outputFile);
                LineWriter writer = new TextLineWriter(outputStream, compressionKind)) {
            for (String value : values) {
                writer.write(Slices.utf8Slice(value));
            }
            assertTrue(writer.getRetainedSizeInBytes() > 0);
        }
    }

    private static void assertOld(File inputFile, List<String> values)
            throws IOException
    {
        JobConf jobConf = new JobConf(false);
        configureCompressionCodecs(jobConf);
        Path path = new Path(inputFile.toURI());

        FileSplit genericSplit = new FileSplit(path, 0, inputFile.length(), (String[]) null);
        try (RecordReader<LongWritable, Text> reader = new TextInputFormat().getRecordReader(genericSplit, jobConf, Reporter.NULL)) {
            LongWritable key = new LongWritable();
            Text text = new Text();
            for (String expected : values) {
                assertTrue(reader.next(key, text));
                assertEquals(text.toString(), expected);
            }

            assertFalse(reader.next(key, text));
        }
    }

    private static void writeOld(File outputFile, Optional<CompressionKind> compressionKind, List<String> values)
            throws Exception
    {
        RecordWriter recordWriter = createWriterOld(outputFile, compressionKind);

        for (String value : values) {
            recordWriter.write(new Text(value));
        }

        recordWriter.close(false);
    }

    private static RecordWriter createWriterOld(File outputFile, Optional<CompressionKind> compressionKind)
            throws IOException
    {
        JobConf jobConf = new JobConf(false);
        Optional<String> codecName = compressionKind.map(CompressionKind::getHadoopClassName);
        codecName.ifPresent(s -> jobConf.set(COMPRESS_CODEC, s));

        return new HiveIgnoreKeyTextOutputFormat<>().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                codecName.isPresent(),
                new Properties(),
                () -> {});
    }

    public static class TempFileWithExtension
            implements Closeable
    {
        private final java.nio.file.Path tempDir;
        private final File file;

        private TempFileWithExtension(String fileExtension)
                throws IOException
        {
            tempDir = createTempDirectory(null);
            file = tempDir.resolve("data.txt" + fileExtension).toFile();
        }

        public File file()
        {
            return file;
        }

        @Override
        public void close()
                throws IOException
        {
            // hadoop creates crc files that must be deleted also, so just delete the whole directory
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }
}
