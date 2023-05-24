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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static io.airlift.testing.Closeables.closeAll;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true) // e.g. test methods operate on shared, mutated tempFile
public class TestFSDataInputStreamTail
{
    private File tempRoot;
    private Path tempFile;
    private RawLocalFileSystem fs;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        fs = new RawLocalFileSystem();
        fs.initialize(fs.getUri(), newEmptyConfiguration());
        tempRoot = Files.createTempDirectory("test_fsdatainputstream_tail").toFile();
        tempFile = new Path(Files.createTempFile(tempRoot.toPath(), "tempfile", "txt").toUri());
    }

    @BeforeMethod
    public void truncateTempFile()
            throws Exception
    {
        // Truncate contents
        fs.truncate(tempFile, 0);
    }

    @AfterClass(alwaysRun = true)
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
            assertEquals(tail.getFileSize(), 0);
            assertEquals(tail.getTailSlice().length(), 0);
            assertSame(tail.getTailSlice(), Slices.EMPTY_SLICE);
        }
    }

    @Test(dataProvider = "validFileSizeAndPaddedFileSize")
    public void testReadTailForFileSize(int fileSize, int paddedFileSize)
            throws Exception
    {
        // Cleanup between each input run
        fs.truncate(tempFile, 0);

        if (fileSize > 0) {
            try (FSDataOutputStream os = fs.append(tempFile)) {
                os.write(countingTestFileContentsWithLength(fileSize));
            }
        }

        assertEquals(fs.getFileLinkStatus(tempFile).getLen(), fileSize);

        try (FSDataInputStream is = fs.open(tempFile)) {
            assertEquals(FSDataInputStreamTail.readTailForFileSize(tempFile.toString(), paddedFileSize, is), fileSize);
        }
    }

    @Test(dataProvider = "validFileSizeAndPaddedFileSize")
    public void testReadTailCompletely(int fileSize, int paddedFileSize)
            throws Exception
    {
        byte[] contents = countingTestFileContentsWithLength(fileSize);
        if (contents.length > 0) {
            try (FSDataOutputStream os = fs.append(tempFile)) {
                os.write(contents);
            }
        }

        assertEquals(fs.getFileLinkStatus(tempFile).getLen(), fileSize);

        try (FSDataInputStream is = fs.open(tempFile)) {
            FSDataInputStreamTail tail = FSDataInputStreamTail.readTail(tempFile.toString(), paddedFileSize, is, fileSize);
            assertEquals(tail.getFileSize(), fileSize);
            Slice tailSlice = tail.getTailSlice();
            assertEquals(tailSlice.length(), fileSize);
            assertCountingTestFileContents(tailSlice.getBytes());
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

        assertEquals(fs.getFileLinkStatus(tempFile).getLen(), contents.length);

        try (FSDataInputStream is = fs.open(tempFile)) {
            FSDataInputStreamTail tail = FSDataInputStreamTail.readTail(tempFile.toString(), contents.length + 32, is, 16);
            assertEquals(tail.getFileSize(), contents.length);
            Slice tailSlice = tail.getTailSlice();

            assertTrue(tailSlice.length() < contents.length);
            assertEquals(tailSlice.getBytes(), Arrays.copyOfRange(contents, contents.length - tailSlice.length(), contents.length));
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

        assertEquals(fs.getFileLinkStatus(tempFile).getLen(), contents.length);

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

        assertEquals(fs.getFileLinkStatus(tempFile).getLen(), contents.length);

        try (FSDataInputStream is = fs.open(tempFile)) {
            assertThatThrownBy(() -> FSDataInputStreamTail.readTailForFileSize(tempFile.toString(), 128, is))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Incorrect file size (128) for file (end of stream not reached): " + tempFile);
        }
    }

    @DataProvider(name = "validFileSizeAndPaddedFileSize")
    public static Object[][] validFileSizeAndPaddedFileSize()
    {
        return new Object[][] {
                {0, 0},
                {0, 1},
                {0, 15},
                {0, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES - 1},
                {0, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES},
                {63, 63},
                {63, 64},
                {64, 74},
                {65, 65 + FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES}};
    }

    private static void assertCountingTestFileContents(byte[] contents)
    {
        assertEquals(contents, countingTestFileContentsWithLength(contents.length));
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
