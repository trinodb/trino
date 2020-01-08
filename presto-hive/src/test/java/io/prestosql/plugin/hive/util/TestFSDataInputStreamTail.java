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

package io.prestosql.plugin.hive.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.plugin.hive.util.FSDataInputStreamTail.ByteArraySeekablePositionedReadableInputStream;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
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

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
        fs.initialize(fs.getUri(), new Configuration(false));
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
        try {
            fs.delete(tempFile, true);
        }
        catch (IOException ignored) {
        }
        try {
            fs.delete(new Path(tempRoot.toURI()), true);
        }
        finally {
            fs.close();
        }
    }

    @Test
    public void testEmptyFileReadTail()
            throws Exception
    {
        try (FSDataInputStream is = fs.open(tempFile)) {
            FSDataInputStreamTail tail = FSDataInputStreamTail.readTail(tempFile, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES, is, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES);
            assertEquals(tail.getFileSize(), 0);
            assertEquals(tail.getTailSlice().length(), 0);
            assertSame(tail.getTailSlice(), Slices.EMPTY_SLICE);

            FSDataInputStream replacement = tail.replaceStreamWithContentsIfComplete(is);
            assertNotSame(replacement, is);
            assertEquals(FSDataInputStreamTail.readTailForFileSize(tempFile, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES, replacement), 0);
            replacement.close();
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
            assertEquals(FSDataInputStreamTail.readTailForFileSize(tempFile, paddedFileSize, is), fileSize);
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
            FSDataInputStreamTail tail = FSDataInputStreamTail.readTail(tempFile, paddedFileSize, is, fileSize);
            assertEquals(tail.getFileSize(), fileSize);
            Slice tailSlice = tail.getTailSlice();
            assertEquals(tailSlice.length(), fileSize);
            assertCountingTestFileContents(tailSlice.getBytes());

            FSDataInputStream replacement = tail.replaceStreamWithContentsIfComplete(is);
            assertNotSame(replacement, is);
            if (fileSize > 0) {
                byte[] replacedRead = new byte[contents.length];
                replacement.readFully(0, replacedRead);
                assertCountingTestFileContents(replacedRead);
            }
            replacement.close();
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
            FSDataInputStreamTail tail = FSDataInputStreamTail.readTail(tempFile, contents.length + 32, is, 16);
            assertEquals(tail.getFileSize(), contents.length);
            Slice tailSlice = tail.getTailSlice();

            assertTrue(tailSlice.length() < contents.length);
            assertEquals(tailSlice.getBytes(), Arrays.copyOfRange(contents, contents.length - tailSlice.length(), contents.length));

            assertSame(tail.replaceStreamWithContentsIfComplete(is), is);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Incorrect fileSize 128 for file .*, end of stream not reached")
    public void testReadTailNoEndOfFileFound()
            throws Exception
    {
        byte[] contents = countingTestFileContentsWithLength(256);
        try (FSDataOutputStream os = fs.append(tempFile)) {
            os.write(contents);
        }

        assertEquals(fs.getFileLinkStatus(tempFile).getLen(), contents.length);

        try (FSDataInputStream is = fs.open(tempFile)) {
            FSDataInputStreamTail.readTail(tempFile, 128, is, 16);
            fail("Expected failure to find end of stream");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), HIVE_FILESYSTEM_ERROR.toErrorCode());
            throw e;
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Incorrect fileSize 128 for file .*, end of stream not reached")
    public void testReadTailForFileSizeNoEndOfFileFound()
            throws Exception
    {
        byte[] contents = countingTestFileContentsWithLength(256);
        try (FSDataOutputStream os = fs.append(tempFile)) {
            os.write(contents);
        }

        assertEquals(fs.getFileLinkStatus(tempFile).getLen(), contents.length);

        try (FSDataInputStream is = fs.open(tempFile)) {
            FSDataInputStreamTail.readTailForFileSize(tempFile, 128, is);
            fail("Expected failure to find end of stream");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), HIVE_FILESYSTEM_ERROR.toErrorCode());
            throw e;
        }
    }

    @Test
    public void testByteArraySeekablePositionedReadableInputStreamRead()
            throws Exception
    {
        byte[] contents = countingTestFileContentsWithLength(256);
        ByteArraySeekablePositionedReadableInputStream stream = wrapToByteArrayStream(contents.clone());

        byte[] check = new byte[contents.length];
        stream.readFully(0, check);
        assertEquals(check, contents);

        check = new byte[2];
        assertEquals(stream.read(255, check, 0, check.length), 1);
        assertEquals(check[0], contents[255]);

        for (int i = 0; i < contents.length; i++) {
            stream.seek(i);
            assertEquals(stream.read(), contents[i]);
        }

        stream.seek(contents.length);
        assertEquals(stream.read(), -1);
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = FSExceptionMessages.CANNOT_SEEK_PAST_EOF)
    public void testByteArraySeekablePositionedReadableInputStreamSeekEOF()
            throws Exception
    {
        wrapToByteArrayStream(new byte[1]).seek(2);
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = FSExceptionMessages.EOF_IN_READ_FULLY)
    public void testByteArraySeekablePositionedReadableInputStreamReadFullyEOF()
            throws Exception
    {
        wrapToByteArrayStream(new byte[1]).readFully(0, new byte[2]);
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

    private static ByteArraySeekablePositionedReadableInputStream wrapToByteArrayStream(byte[] contents)
    {
        return new ByteArraySeekablePositionedReadableInputStream(contents, 0, contents.length);
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
