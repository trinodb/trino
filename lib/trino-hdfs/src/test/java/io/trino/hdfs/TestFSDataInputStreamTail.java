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

package io.trino.hdfs;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static io.airlift.testing.Closeables.closeAll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // e.g. test methods operate on shared, mutated tempFile
public class TestFSDataInputStreamTail
{
    private File tempRoot;
    private Path tempFile;
    private RawLocalFileSystem fs;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        fs = new RawLocalFileSystem();
        fs.initialize(fs.getUri(), new Configuration(false));
        tempRoot = Files.createTempDirectory("test_fsdatainputstream_tail").toFile();
        tempFile = new Path(Files.createTempFile(tempRoot.toPath(), "tempfile", "txt").toUri());
    }

    @BeforeEach
    public void truncateTempFile()
            throws Exception
    {
        // Truncate contents
        fs.truncate(tempFile, 0);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closeAll(
                () -> fs.delete(new Path(tempRoot.toURI()), true),
                fs);
        fs = null;
    }

    @Test
    public void testEmptyFileReadTail()
            throws Exception
    {
        try (FSDataInputStream is = fs.open(tempFile)) {
            FSDataInputStreamTail tail = FSDataInputStreamTail.readTail(tempFile.toString(), FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES, is, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES);
            assertThat(tail.getFileSize()).isEqualTo(0);
            assertThat(tail.getTailSlice().length()).isEqualTo(0);
            assertThat(tail.getTailSlice()).isSameAs(Slices.EMPTY_SLICE);
        }
    }

    @Test
    public void testReadTailForFileSize()
            throws Exception
    {
        testReadTailForFileSize(0, 0);
        testReadTailForFileSize(0, 1);
        testReadTailForFileSize(0, 15);
        testReadTailForFileSize(0, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES - 1);
        testReadTailForFileSize(0, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES);
        testReadTailForFileSize(63, 63);
        testReadTailForFileSize(63, 64);
        testReadTailForFileSize(64, 74);
        testReadTailForFileSize(65, 65 + FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES);
    }

    private void testReadTailForFileSize(int fileSize, int paddedFileSize)
            throws Exception
    {
        // Cleanup between each input run
        fs.truncate(tempFile, 0);

        if (fileSize > 0) {
            try (FSDataOutputStream os = fs.append(tempFile)) {
                os.write(countingTestFileContentsWithLength(fileSize));
            }
        }

        assertThat(fs.getFileLinkStatus(tempFile).getLen()).isEqualTo(fileSize);

        try (FSDataInputStream is = fs.open(tempFile)) {
            assertThat(FSDataInputStreamTail.readTailForFileSize(tempFile.toString(), paddedFileSize, is)).isEqualTo(fileSize);
        }
    }

    @Test
    public void testReadTailCompletely()
            throws Exception
    {
        testReadTailCompletely(0, 0);
        testReadTailCompletely(0, 1);
        testReadTailCompletely(0, 15);
        testReadTailCompletely(0, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES - 1);
        testReadTailCompletely(0, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES);
        testReadTailCompletely(63, 63);
        testReadTailCompletely(63, 64);
        testReadTailCompletely(64, 74);
        testReadTailCompletely(65, 65 + FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES);
    }

    private void testReadTailCompletely(int fileSize, int paddedFileSize)
            throws Exception
    {
        fs.truncate(tempFile, 0);

        byte[] contents = countingTestFileContentsWithLength(fileSize);
        if (contents.length > 0) {
            try (FSDataOutputStream os = fs.append(tempFile)) {
                os.write(contents);
            }
        }

        assertThat(fs.getFileLinkStatus(tempFile).getLen()).isEqualTo(fileSize);

        try (FSDataInputStream is = fs.open(tempFile)) {
            FSDataInputStreamTail tail = FSDataInputStreamTail.readTail(tempFile.toString(), paddedFileSize, is, fileSize);
            assertThat(tail.getFileSize()).isEqualTo(fileSize);
            Slice tailSlice = tail.getTailSlice();
            assertThat(tailSlice.length()).isEqualTo(fileSize);
            byte[] tailContents = tailSlice.getBytes();
            assertThat(tailContents).isEqualTo(countingTestFileContentsWithLength(tailContents.length));
        }
    }

    @Test
    public void testReadTailPartial()
            throws Exception
    {
        byte[] contents = countingTestFileContentsWithLength(256);
        try (FSDataOutputStream os = fs.append(tempFile)) {
            os.write(contents);
        }

        assertThat(fs.getFileLinkStatus(tempFile).getLen()).isEqualTo(contents.length);

        try (FSDataInputStream is = fs.open(tempFile)) {
            FSDataInputStreamTail tail = FSDataInputStreamTail.readTail(tempFile.toString(), contents.length + 32, is, 16);
            assertThat(tail.getFileSize()).isEqualTo(contents.length);
            Slice tailSlice = tail.getTailSlice();

            assertThat(tailSlice.length() < contents.length).isTrue();
            assertThat(tailSlice.getBytes()).isEqualTo(Arrays.copyOfRange(contents, contents.length - tailSlice.length(), contents.length));
        }
    }

    @Test
    public void testReadTailNoEndOfFileFound()
            throws Exception
    {
        byte[] contents = countingTestFileContentsWithLength(256);
        try (FSDataOutputStream os = fs.append(tempFile)) {
            os.write(contents);
        }

        assertThat(fs.getFileLinkStatus(tempFile).getLen()).isEqualTo(contents.length);

        try (FSDataInputStream is = fs.open(tempFile)) {
            assertThatThrownBy(() -> FSDataInputStreamTail.readTail(tempFile.toString(), 128, is, 16))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Incorrect file size (128) for file (end of stream not reached): " + tempFile);
        }
    }

    @Test
    public void testReadTailForFileSizeNoEndOfFileFound()
            throws Exception
    {
        byte[] contents = countingTestFileContentsWithLength(256);
        try (FSDataOutputStream os = fs.append(tempFile)) {
            os.write(contents);
        }

        assertThat(fs.getFileLinkStatus(tempFile).getLen()).isEqualTo(contents.length);

        try (FSDataInputStream is = fs.open(tempFile)) {
            assertThatThrownBy(() -> FSDataInputStreamTail.readTailForFileSize(tempFile.toString(), 128, is))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Incorrect file size (128) for file (end of stream not reached): " + tempFile);
        }
    }

    private static byte[] countingTestFileContentsWithLength(int length)
    {
        byte[] contents = new byte[length];
        for (int i = 0, b = 0; i < contents.length; i++, b++) {
            if (b > Byte.MAX_VALUE) {
                b = 0;
            }
            contents[i] = (byte) b;
        }
        return contents;
    }
}
