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
package io.trino.filesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.trino.filesystem.encryption.EncryptionEnforcingFileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.parallel.Execution;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(Lifecycle.PER_CLASS)
@Execution(SAME_THREAD)
public abstract class AbstractTestTrinoFileSystem
{
    protected static final String TEST_BLOB_CONTENT_PREFIX = "test blob content for ";
    private static final int MEGABYTE = 1024 * 1024;

    protected abstract boolean isHierarchical();

    protected abstract TrinoFileSystem getFileSystem();

    protected abstract Location getRootLocation();

    protected abstract void verifyFileSystemIsEmpty();

    protected boolean useServerSideEncryptionWithCustomerKey()
    {
        return false;
    }

    /**
     * Specifies whether implementation {@link TrinoOutputFile#create()} is exclusive.
     */
    protected boolean isCreateExclusive()
    {
        return true;
    }

    /**
     * Specifies whether implementation supports {@link TrinoOutputFile#createExclusive(byte[])}.
     */
    protected boolean supportsCreateExclusive()
    {
        return true;
    }

    protected boolean supportsRenameFile()
    {
        return true;
    }

    protected boolean supportsIncompleteWriteNoClobber()
    {
        return true;
    }

    protected boolean supportsPreSignedUri()
    {
        return false;
    }

    protected boolean normalizesListFilesResult()
    {
        return false;
    }

    protected boolean seekPastEndOfFileFails()
    {
        return true;
    }

    protected Location createLocation(String path)
    {
        if (path.isEmpty()) {
            return getRootLocation();
        }
        return getRootLocation().appendPath(path);
    }

    @BeforeEach
    void beforeEach()
    {
        verifyFileSystemIsEmpty();
    }

    @Test
    void testInputFileMetadata()
            throws IOException
    {
        // an input file cannot be created at the root of the file system
        assertThatThrownBy(() -> getFileSystem().newInputFile(getRootLocation()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation().toString());
        assertThatThrownBy(() -> getFileSystem().newInputFile(Location.of(getRootLocation() + "/")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation().toString() + "/");
        // an input file location cannot end with a slash
        assertThatThrownBy(() -> getFileSystem().newInputFile(createLocation("foo/")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(createLocation("foo/").toString());

        try (TempBlob tempBlob = randomBlobLocation("inputFileMetadata")) {
            TrinoInputFile inputFile = getFileSystem().newInputFile(tempBlob.location());
            assertThat(inputFile.location()).isEqualTo(tempBlob.location());
            assertThat(inputFile.exists()).isFalse();

            // getting length or modified time of non-existent file is an error
            assertThatThrownBy(inputFile::length)
                    .isInstanceOf(FileNotFoundException.class)
                    .hasMessageContaining(tempBlob.location().toString());
            assertThatThrownBy(inputFile::lastModified)
                    .isInstanceOf(FileNotFoundException.class)
                    .hasMessageContaining(tempBlob.location().toString());

            tempBlob.createOrOverwrite("123456");

            assertThat(inputFile.length()).isEqualTo(6);
            Instant lastModified = inputFile.lastModified();
            assertThat(lastModified).isEqualTo(tempBlob.inputFile().lastModified());

            // delete file and verify that exists check is not cached
            tempBlob.close();
            assertThat(inputFile.exists()).isFalse();
            // input file caches metadata, so results will be unchanged after delete
            assertThat(inputFile.length()).isEqualTo(6);
            assertThat(inputFile.lastModified()).isEqualTo(lastModified);
        }
    }

    @Test
    void testInputFileWithLengthMetadata()
            throws IOException
    {
        // an input file cannot be created at the root of the file system
        assertThatThrownBy(() -> getFileSystem().newInputFile(getRootLocation(), 22))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation().toString());
        assertThatThrownBy(() -> getFileSystem().newInputFile(Location.of(getRootLocation() + "/"), 22))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation() + "/");
        // an input file location cannot end with a slash
        assertThatThrownBy(() -> getFileSystem().newInputFile(createLocation("foo/"), 22))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(createLocation("foo/").toString());

        try (TempBlob tempBlob = randomBlobLocation("inputFileWithLengthMetadata")) {
            TrinoInputFile inputFile = getFileSystem().newInputFile(tempBlob.location(), 22);
            assertThat(inputFile.exists()).isFalse();

            // getting length for non-existent file returns pre-declared length
            assertThat(inputFile.length()).isEqualTo(22);
            // modified time of non-existent file is an error
            assertThatThrownBy(inputFile::lastModified)
                    .isInstanceOf(FileNotFoundException.class)
                    .hasMessageContaining(tempBlob.location().toString());
            // double-check the length did not change in call above
            assertThat(inputFile.length()).isEqualTo(22);

            tempBlob.createOrOverwrite("123456");

            // length always returns the pre-declared length
            assertThat(inputFile.length()).isEqualTo(22);
            // modified time works
            Instant lastModified = inputFile.lastModified();
            assertThat(lastModified).isEqualTo(tempBlob.inputFile().lastModified());
            // double-check the length did not change when metadata was loaded
            assertThat(inputFile.length()).isEqualTo(22);

            // delete file and verify that exists check is not cached
            tempBlob.close();
            assertThat(inputFile.exists()).isFalse();
            // input file caches metadata, so results will be unchanged after delete
            assertThat(inputFile.length()).isEqualTo(22);
            assertThat(inputFile.lastModified()).isEqualTo(lastModified);
        }
    }

    @Test
    void testInputFileWithLastModifiedMetadata()
            throws IOException
    {
        try (TempBlob tempBlob = randomBlobLocation("inputFileWithLastModifiedMetadata")) {
            TrinoInputFile inputFile = getFileSystem().newInputFile(tempBlob.location(), 22, Instant.ofEpochMilli(12345));
            assertThat(inputFile.exists()).isFalse();

            // getting length for non-existent file returns pre-declared length
            assertThat(inputFile.length()).isEqualTo(22);
            // getting modified time for non-existent file returns pre-declared modified time
            assertThat(inputFile.lastModified()).isEqualTo(Instant.ofEpochMilli(12345));
            // double-check the length did not change in call above
            assertThat(inputFile.length()).isEqualTo(22);

            tempBlob.createOrOverwrite("123456");

            // length always returns the pre-declared length
            assertThat(inputFile.length()).isEqualTo(22);
            // modified time always returns the pre-declared length
            assertThat(inputFile.lastModified()).isEqualTo(Instant.ofEpochMilli(12345));
            // double-check the length did not change when metadata was loaded
            assertThat(inputFile.length()).isEqualTo(22);

            // delete file and verify that exists check is not cached
            tempBlob.close();
            assertThat(inputFile.exists()).isFalse();
            // input file caches metadata, so results will be unchanged after delete
            assertThat(inputFile.length()).isEqualTo(22);
            assertThat(inputFile.lastModified()).isEqualTo(Instant.ofEpochMilli(12345));
        }
    }

    @Test
    public void testInputFile()
            throws IOException
    {
        try (TempBlob tempBlob = randomBlobLocation("inputStream")) {
            // creating an input file for a non-existent file succeeds
            TrinoInputFile inputFile = getFileSystem().newInputFile(tempBlob.location());

            // reading a non-existent file is an error
            assertThatThrownBy(
                    () -> {
                        try (TrinoInputStream inputStream = inputFile.newStream()) {
                            inputStream.readAllBytes();
                        }
                    })
                    .isInstanceOf(FileNotFoundException.class)
                    .hasMessageContaining(tempBlob.location().toString());
            assertThatThrownBy(
                    () -> {
                        try (TrinoInput input = inputFile.newInput()) {
                            input.readFully(0, 10);
                        }
                    })
                    .isInstanceOf(FileNotFoundException.class)
                    .hasMessageContaining(tempBlob.location().toString());
            assertThatThrownBy(
                    () -> {
                        try (TrinoInput input = inputFile.newInput()) {
                            input.readTail(10);
                        }
                    })
                    .isInstanceOf(FileNotFoundException.class)
                    .hasMessageContaining(tempBlob.location().toString());

            // write a 16 MB file
            try (OutputStream outputStream = tempBlob.outputFile().create()) {
                byte[] bytes = new byte[4];
                Slice slice = wrappedBuffer(bytes);
                for (int i = 0; i < 4 * MEGABYTE; i++) {
                    slice.setInt(0, i);
                    outputStream.write(bytes);
                }
            }

            int fileSize = 16 * MEGABYTE;
            assertThat(inputFile.exists()).isTrue();
            assertThat(inputFile.length()).isEqualTo(fileSize);

            try (TrinoInputStream inputStream = inputFile.newStream()) {
                byte[] bytes = new byte[4];
                Slice slice = wrappedBuffer(bytes);

                // read int at a time
                for (int intPosition = 0; intPosition < 4 * MEGABYTE; intPosition++) {
                    assertThat(inputStream.getPosition()).isEqualTo(intPosition * 4L);

                    int size = inputStream.readNBytes(bytes, 0, bytes.length);
                    assertThat(size).isEqualTo(4);
                    assertThat(slice.getInt(0)).isEqualTo(intPosition);
                    assertThat(inputStream.getPosition()).isEqualTo((intPosition * 4) + size);
                }
                assertThat(inputStream.getPosition()).isEqualTo(fileSize);
                assertThat(inputStream.read()).isLessThan(0);
                assertThat(inputStream.read(bytes)).isLessThan(0);
                if (seekPastEndOfFileFails()) {
                    assertThat(inputStream.skip(10)).isEqualTo(0);
                }
                else {
                    assertThat(inputStream.skip(10)).isEqualTo(10L);
                }

                // seek 4 MB in and read byte at a time
                inputStream.seek(4 * MEGABYTE);
                for (int intPosition = MEGABYTE; intPosition < 4 * MEGABYTE; intPosition++) {
                    // write i into bytes, for validation below
                    slice.setInt(0, intPosition);
                    for (byte b : bytes) {
                        int value = inputStream.read();
                        assertThat(value).isGreaterThanOrEqualTo(0);
                        assertThat((byte) value).isEqualTo(b);
                    }
                }
                assertThat(inputStream.getPosition()).isEqualTo(fileSize);
                assertThat(inputStream.read()).isLessThan(0);
                assertThat(inputStream.read(bytes)).isLessThan(0);
                if (seekPastEndOfFileFails()) {
                    assertThat(inputStream.skip(10)).isEqualTo(0);
                }
                else {
                    assertThat(inputStream.skip(10)).isEqualTo(10L);
                }

                // seek 1MB at a time
                for (int i = 0; i < 16; i++) {
                    int expectedPosition = i * MEGABYTE;
                    inputStream.seek(expectedPosition);
                    assertThat(inputStream.getPosition()).isEqualTo(expectedPosition);

                    int size = inputStream.readNBytes(bytes, 0, bytes.length);
                    assertThat(size).isEqualTo(4);
                    assertThat(slice.getInt(0)).isEqualTo(expectedPosition / 4);
                }

                // skip 1MB at a time
                inputStream.seek(0);
                long expectedPosition = 0;
                for (int i = 0; i < 15; i++) {
                    long skipSize = inputStream.skip(MEGABYTE);
                    assertThat(skipSize).isEqualTo(MEGABYTE);
                    expectedPosition += skipSize;
                    assertThat(inputStream.getPosition()).isEqualTo(expectedPosition);

                    int size = inputStream.readNBytes(bytes, 0, bytes.length);
                    assertThat(size).isEqualTo(4);
                    assertThat(slice.getInt(0)).isEqualTo(expectedPosition / 4);
                    expectedPosition += size;
                }
                if (seekPastEndOfFileFails()) {
                    long skipSize = inputStream.skip(MEGABYTE);
                    assertThat(skipSize).isEqualTo(fileSize - expectedPosition);
                    assertThat(inputStream.getPosition()).isEqualTo(fileSize);
                }

                // skip N bytes
                inputStream.seek(0);
                expectedPosition = 0;
                for (int i = 1; i <= 11; i++) {
                    int size = min((MEGABYTE / 4) * i, MEGABYTE * 2);
                    inputStream.skipNBytes(size);
                    expectedPosition += size;
                    assertThat(inputStream.getPosition()).isEqualTo(expectedPosition);

                    size = inputStream.readNBytes(bytes, 0, bytes.length);
                    assertThat(size).isEqualTo(4);
                    assertThat(slice.getInt(0)).isEqualTo(expectedPosition / 4);
                    expectedPosition += size;
                }
                inputStream.skipNBytes(fileSize - expectedPosition);
                assertThat(inputStream.getPosition()).isEqualTo(fileSize);

                if (seekPastEndOfFileFails()) {
                    // skip beyond the end of the file is not allowed
                    inputStream.seek(expectedPosition);
                    assertThat(expectedPosition + MEGABYTE).isGreaterThan(fileSize);
                    assertThatThrownBy(() -> inputStream.skipNBytes(MEGABYTE))
                            .isInstanceOf(EOFException.class);
                }

                inputStream.seek(fileSize);
                if (seekPastEndOfFileFails()) {
                    assertThatThrownBy(() -> inputStream.skipNBytes(1))
                            .isInstanceOf(EOFException.class);
                }

                inputStream.seek(fileSize);
                if (seekPastEndOfFileFails()) {
                    assertThat(inputStream.skip(1)).isEqualTo(0);
                }
                else {
                    assertThat(inputStream.skip(1)).isEqualTo(1L);
                }

                // seek beyond the end of the file, is not allowed
                long currentPosition = fileSize - 500;
                inputStream.seek(currentPosition);
                assertThat(inputStream.read()).isGreaterThanOrEqualTo(0);
                currentPosition++;
                if (seekPastEndOfFileFails()) {
                    assertThatThrownBy(() -> inputStream.seek(fileSize + 100))
                            .isInstanceOf(IOException.class)
                            .hasMessageContaining(tempBlob.location().toString());
                    assertThat(inputStream.getPosition()).isEqualTo(currentPosition);
                    assertThat(inputStream.read()).isGreaterThanOrEqualTo(0);
                    assertThat(inputStream.getPosition()).isEqualTo(currentPosition + 1);
                }
                else {
                    inputStream.seek(fileSize + 100);
                    assertThat(inputStream.getPosition()).isEqualTo(fileSize + 100);
                    assertThat(inputStream.read()).isEqualTo(-1);
                    assertThat(inputStream.readNBytes(50)).isEmpty();
                    assertThat(inputStream.getPosition()).isEqualTo(fileSize + 100);
                }

                assertThatThrownBy(() -> inputStream.read(new byte[1], -1, 0))
                        .isInstanceOf(IndexOutOfBoundsException.class);
                assertThatThrownBy(() -> inputStream.read(new byte[1], 0, -1))
                        .isInstanceOf(IndexOutOfBoundsException.class);
                assertThatThrownBy(() -> inputStream.read(new byte[1], 1, 3))
                        .isInstanceOf(IndexOutOfBoundsException.class);

                // verify all the methods throw after close
                inputStream.close();
                assertThatThrownBy(inputStream::available)
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(() -> inputStream.seek(0))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(inputStream::read)
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(() -> inputStream.read(new byte[10]))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(() -> inputStream.read(new byte[10], 2, 3))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
            }

            try (TrinoInput trinoInput = inputFile.newInput()) {
                byte[] bytes = new byte[4 * 10];
                Slice slice = wrappedBuffer(bytes);

                // positioned read
                trinoInput.readFully(0, bytes, 0, bytes.length);
                for (int i = 0; i < 10; i++) {
                    assertThat(slice.getInt(i * 4)).isEqualTo(i);
                }
                assertThat(trinoInput.readFully(0, bytes.length)).isEqualTo(wrappedBuffer(bytes));

                trinoInput.readFully(0, bytes, 2, bytes.length - 2);
                for (int i = 0; i < 9; i++) {
                    assertThat(slice.getInt(2 + i * 4)).isEqualTo(i);
                }

                trinoInput.readFully(MEGABYTE, bytes, 0, bytes.length);
                for (int i = 0; i < 10; i++) {
                    assertThat(slice.getInt(i * 4)).isEqualTo(i + MEGABYTE / 4);
                }
                assertThat(trinoInput.readFully(MEGABYTE, bytes.length)).isEqualTo(wrappedBuffer(bytes));
                assertThatThrownBy(() -> trinoInput.readFully(fileSize - bytes.length + 1, bytes, 0, bytes.length))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());

                // tail read
                trinoInput.readTail(bytes, 0, bytes.length);
                int totalPositions = 16 * MEGABYTE / 4;
                for (int i = 0; i < 10; i++) {
                    assertThat(slice.getInt(i * 4)).isEqualTo(totalPositions - 10 + i);
                }

                assertThat(trinoInput.readTail(bytes.length)).isEqualTo(wrappedBuffer(bytes));

                trinoInput.readTail(bytes, 2, bytes.length - 2);
                for (int i = 0; i < 9; i++) {
                    assertThat(slice.getInt(4 + i * 4)).isEqualTo(totalPositions - 9 + i);
                }

                // verify all the methods throw after close
                trinoInput.close();
                assertThatThrownBy(() -> trinoInput.readFully(0, 10))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(() -> trinoInput.readFully(0, bytes, 0, 10))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(() -> trinoInput.readTail(10))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(() -> trinoInput.readTail(bytes, 0, 10))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
            }

            // overwrite with an empty file and verify that reading it doesn't throw errors
            tempBlob.outputFile().createOrOverwrite(new byte[0]);
            try (TrinoInputStream inputStream = tempBlob.inputFile().newStream()) {
                assertThat(inputStream.read()).isLessThan(0);
            }
        }
    }

    @Test
    void testOutputFile()
            throws IOException
    {
        // an output file cannot be created at the root of the file system
        assertThatThrownBy(() -> getFileSystem().newOutputFile(getRootLocation()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation().toString());
        assertThatThrownBy(() -> getFileSystem().newOutputFile(Location.of(getRootLocation() + "/")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation() + "/");
        // an output file location cannot end with a slash
        assertThatThrownBy(() -> getFileSystem().newOutputFile(createLocation("foo/")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(createLocation("foo/").toString());

        try (TempBlob tempBlob = randomBlobLocation("outputFile")) {
            TrinoOutputFile outputFile = getFileSystem().newOutputFile(tempBlob.location());
            assertThat(outputFile.location()).isEqualTo(tempBlob.location());
            assertThat(tempBlob.exists()).isFalse();

            // create file and write data
            try (OutputStream outputStream = outputFile.create()) {
                outputStream.write("initial".getBytes(UTF_8));
            }

            if (isCreateExclusive()) {
                // re-create without overwrite is an error
                assertThatThrownBy(() -> outputFile.create().close())
                        .isInstanceOf(FileAlreadyExistsException.class)
                        .hasMessageContaining(tempBlob.location().toString());

                // verify nothing changed
                assertThat(tempBlob.read()).isEqualTo("initial");

                // re-create exclusive is an error
                if (supportsCreateExclusive()) {
                    assertThatThrownBy(() -> outputFile.createExclusive(new byte[0]))
                            .isInstanceOf(FileAlreadyExistsException.class)
                            .hasMessageContaining(tempBlob.location().toString());
                }
                else {
                    assertThatThrownBy(() -> outputFile.createExclusive(new byte[0]))
                            .isInstanceOf(UnsupportedOperationException.class)
                            .hasMessageStartingWith("createExclusive not supported");
                }

                // verify nothing changed
                assertThat(tempBlob.read()).isEqualTo("initial");
            }
            else {
                // re-create without overwrite succeeds
                try (OutputStream outputStream = outputFile.create()) {
                    outputStream.write("replaced".getBytes(UTF_8));
                }

                // verify contents changed
                assertThat(tempBlob.read()).isEqualTo("replaced");

                // create exclusive is an error
                if (supportsCreateExclusive()) {
                    assertThatThrownBy(() -> outputFile.createExclusive(new byte[0]))
                            .isInstanceOf(FileAlreadyExistsException.class)
                            .hasMessageContaining(tempBlob.location().toString());
                }
                else {
                    assertThatThrownBy(() -> outputFile.createExclusive(new byte[0]))
                            .isInstanceOf(UnsupportedOperationException.class)
                            .hasMessageStartingWith("createExclusive not supported");
                }
            }

            // overwrite file
            outputFile.createOrOverwrite("overwrite".getBytes(UTF_8));

            // verify file is different
            assertThat(tempBlob.read()).isEqualTo("overwrite");
        }
    }

    @Test
    void testCreateExclusiveIsAtomic()
            throws Exception
    {
        if (!supportsCreateExclusive()) {
            return;
        }

        int timeoutSeconds = 20;
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("testCreateExclusiveIsAtomic-%s"));
        AtomicBoolean finishing = new AtomicBoolean(false);
        try (TempBlob tempBlob = randomBlobLocation("outputFile")) {
            TrinoFileSystem fileSystem = getFileSystem();
            byte[] content = "a".repeat(MEGABYTE).getBytes(US_ASCII);

            fileSystem.deleteFile(tempBlob.location());
            CyclicBarrier barrier = new CyclicBarrier(2);
            Future<?> write = executor.submit(() -> {
                barrier.await(timeoutSeconds, SECONDS);
                fileSystem.newOutputFile(tempBlob.location()).createExclusive(content);
                return null;
            });
            Future<Slice> read = executor.submit(() -> {
                TrinoInputFile inputFile = fileSystem.newInputFile(tempBlob.location());
                assertThat(inputFile.exists()).as("inputFile.exists()").isFalse();
                barrier.await(timeoutSeconds, SECONDS);

                while (!finishing.get()) {
                    try (TrinoInput input = inputFile.newInput()) {
                        return input.readFully(0, content.length);
                    }
                    catch (FileNotFoundException expected) {
                    }
                }
                throw new RuntimeException("File not created");
            });

            assertThat(read.get(timeoutSeconds, SECONDS).getBytes()).as("read content").isEqualTo(content);
            write.get(timeoutSeconds, SECONDS);
        }
        finally {
            finishing.set(true);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(timeoutSeconds, SECONDS)).as("executor terminated").isTrue();
        }
    }

    @Test
    void testOutputStreamByteAtATime()
            throws IOException
    {
        try (TempBlob tempBlob = randomBlobLocation("inputStream")) {
            try (OutputStream outputStream = tempBlob.outputFile().create()) {
                for (int i = 0; i < MEGABYTE; i++) {
                    outputStream.write(i);
                    if (i % 1024 == 0) {
                        outputStream.flush();
                    }
                }
                outputStream.close();

                // verify all the methods throw after close
                assertThatThrownBy(() -> outputStream.write(42))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(() -> outputStream.write(new byte[10]))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(() -> outputStream.write(new byte[10], 1, 3))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
                assertThatThrownBy(outputStream::flush)
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(tempBlob.location().toString());
            }

            try (TrinoInputStream inputStream = tempBlob.inputFile().newStream()) {
                for (int i = 0; i < MEGABYTE; i++) {
                    int value = inputStream.read();
                    assertThat(value).isGreaterThanOrEqualTo(0);
                    assertThat((byte) value).isEqualTo((byte) i);
                }
            }
        }
    }

    @Test
    void testOutputStreamLargeWrites()
            throws IOException
    {
        try (TempBlob tempBlob = randomBlobLocation("inputStream")) {
            try (OutputStream outputStream = tempBlob.outputFile().create()) {
                for (int i = 0; i < 8; i++) {
                    byte[] bytes = new byte[MEGABYTE / 2];
                    Arrays.fill(bytes, (byte) i);
                    outputStream.write(bytes);
                }
            }

            try (TrinoInputStream inputStream = tempBlob.inputFile().newStream()) {
                for (int i = 0; i < 8; i++) {
                    byte[] expected = new byte[MEGABYTE / 2];
                    Arrays.fill(expected, (byte) i);
                    byte[] actual = inputStream.readNBytes(expected.length);
                    assertThat(actual.length).isEqualTo(expected.length);
                    assertThat(actual).isEqualTo(expected);
                }
            }
        }
    }

    @Test
    public void testPaths()
            throws IOException
    {
        if (isHierarchical()) {
            testPathHierarchical();
        }
        else {
            testPathBlob();
        }
    }

    protected void testPathHierarchical()
            throws IOException
    {
        // file outside of root is not allowed
        // the check is over the entire statement, because some file system delay path checks until the data is uploaded
        assertThatThrownBy(() -> getFileSystem().newOutputFile(createLocation("../file")).createOrOverwrite("test".getBytes(UTF_8)))
                .isInstanceOfAny(IOException.class, IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());

        try (TempBlob absolute = new TempBlob(createLocation("b"))) {
            try (TempBlob alias = new TempBlob(createLocation("a/../b"))) {
                absolute.createOrOverwrite(TEST_BLOB_CONTENT_PREFIX + absolute.location().toString());
                assertThat(alias.exists()).isTrue();
                assertThat(absolute.exists()).isTrue();

                assertThat(alias.read()).isEqualTo(TEST_BLOB_CONTENT_PREFIX + absolute.location().toString());

                assertThat(listPath("")).containsExactly(absolute.location());

                getFileSystem().deleteFile(alias.location());
                assertThat(alias.exists()).isFalse();
                assertThat(absolute.exists()).isFalse();
            }
        }
    }

    protected void testPathBlob()
            throws IOException
    {
        try (TempBlob tempBlob = new TempBlob(createLocation("test/.././/file"))) {
            TrinoInputFile inputFile = getFileSystem().newInputFile(tempBlob.location());
            assertThat(inputFile.location()).isEqualTo(tempBlob.location());
            assertThat(inputFile.exists()).isFalse();

            tempBlob.createOrOverwrite(TEST_BLOB_CONTENT_PREFIX + tempBlob.location().toString());
            assertThat(inputFile.length()).isEqualTo(TEST_BLOB_CONTENT_PREFIX.length() + tempBlob.location().toString().length());
            assertThat(tempBlob.read()).isEqualTo(TEST_BLOB_CONTENT_PREFIX + tempBlob.location().toString());

            if (!normalizesListFilesResult()) {
                assertThat(listPath("test/..")).containsExactly(tempBlob.location());
            }

            if (supportsRenameFile()) {
                getFileSystem().renameFile(tempBlob.location(), createLocation("file"));
                assertThat(inputFile.exists()).isFalse();
                assertThat(readLocation(createLocation("file"))).isEqualTo(TEST_BLOB_CONTENT_PREFIX + tempBlob.location().toString());

                getFileSystem().renameFile(createLocation("file"), tempBlob.location());
                assertThat(inputFile.exists()).isTrue();
                assertThat(tempBlob.read()).isEqualTo(TEST_BLOB_CONTENT_PREFIX + tempBlob.location().toString());
            }

            getFileSystem().deleteFile(tempBlob.location());
            assertThat(inputFile.exists()).isFalse();
        }
    }

    @Test
    void testDeleteFile()
            throws IOException
    {
        // delete file location cannot be the root of the file system
        assertThatThrownBy(() -> getFileSystem().deleteFile(getRootLocation()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation().toString());
        assertThatThrownBy(() -> getFileSystem().deleteFile(Location.of(getRootLocation() + "/")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation() + "/");
        // delete file location cannot end with a slash
        assertThatThrownBy(() -> getFileSystem().deleteFile(createLocation("foo/")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(createLocation("foo/").toString());

        try (TempBlob tempBlob = randomBlobLocation("delete")) {
            // deleting a non-existent file is a no-op
            getFileSystem().deleteFile(tempBlob.location());

            tempBlob.createOrOverwrite("delete me");

            getFileSystem().deleteFile(tempBlob.location());
            assertThat(tempBlob.exists()).isFalse();
        }
    }

    @Test
    void testDeleteFiles()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            Set<Location> locations = createTestDirectoryStructure(closer, isHierarchical());

            getFileSystem().deleteFiles(locations);
            for (Location location : locations) {
                assertThat(getFileSystem().newInputFile(location).exists()).isFalse();
            }
        }
    }

    @Test
    public void testDeleteDirectory()
            throws IOException
    {
        testDeleteDirectory(isHierarchical());
    }

    protected void testDeleteDirectory(boolean hierarchicalNamingConstraints)
            throws IOException
    {
        // for safety make sure the file system is empty before deleting directories
        verifyFileSystemIsEmpty();

        try (Closer closer = Closer.create()) {
            Set<Location> locations = createTestDirectoryStructure(closer, hierarchicalNamingConstraints);

            // for safety make sure the verification code is functioning
            assertThatThrownBy(this::verifyFileSystemIsEmpty)
                    .isInstanceOf(Throwable.class);

            // delete directory on a file is a noop
            getFileSystem().deleteDirectory(createLocation("unknown"));
            for (Location location : locations) {
                assertThat(getFileSystem().newInputFile(location).exists()).isTrue();
            }

            if (isHierarchical()) {
                // delete directory cannot be called on a file
                assertThatThrownBy(() -> getFileSystem().deleteDirectory(createLocation("level0-file0")))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(createLocation("level0-file0").toString());
            }

            getFileSystem().deleteDirectory(createLocation("level0"));
            Location deletedLocationPrefix = createLocation("level0/");
            for (Location location : Ordering.usingToString().sortedCopy(locations)) {
                assertThat(getFileSystem().newInputFile(location).exists()).as("%s exists", location)
                        .isEqualTo(!location.toString().startsWith(deletedLocationPrefix.toString()));
            }

            getFileSystem().deleteDirectory(getRootLocation());
            for (Location location : locations) {
                assertThat(getFileSystem().newInputFile(location).exists()).isFalse();
            }
        }
    }

    @Test
    protected void testRenameFile()
            throws IOException
    {
        if (!supportsRenameFile()) {
            try (TempBlob sourceBlob = randomBlobLocation("renameSource");
                    TempBlob targetBlob = randomBlobLocation("renameTarget")) {
                sourceBlob.createOrOverwrite("data");
                assertThatThrownBy(() -> getFileSystem().renameFile(sourceBlob.location(), targetBlob.location()))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("does not support renames");
            }
            return;
        }

        // rename file locations cannot be the root of the file system
        assertThatThrownBy(() -> getFileSystem().renameFile(getRootLocation(), createLocation("file")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation().toString());
        assertThatThrownBy(() -> getFileSystem().renameFile(createLocation("file"), getRootLocation()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation().toString());
        assertThatThrownBy(() -> getFileSystem().renameFile(Location.of(getRootLocation() + "/"), createLocation("file")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation() + "/");
        assertThatThrownBy(() -> getFileSystem().renameFile(createLocation("file"), Location.of(getRootLocation() + "/")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(getRootLocation() + "/");
        // rename file locations cannot end with a slash
        assertThatThrownBy(() -> getFileSystem().renameFile(createLocation("foo/"), createLocation("file")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(createLocation("foo/").toString());
        assertThatThrownBy(() -> getFileSystem().renameFile(createLocation("file"), createLocation("foo/")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(createLocation("foo/").toString());

        // todo rename to existing file name
        try (TempBlob sourceBlob = randomBlobLocation("renameSource");
                TempBlob targetBlob = randomBlobLocation("renameTarget")) {
            // renaming a non-existent file is an error
            assertThatThrownBy(() -> getFileSystem().renameFile(sourceBlob.location(), targetBlob.location()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(sourceBlob.location().toString())
                    .hasMessageContaining(targetBlob.location().toString());

            // create target directory first
            getFileSystem().createDirectory(targetBlob.location().parentDirectory());

            // rename
            sourceBlob.createOrOverwrite("data");
            getFileSystem().renameFile(sourceBlob.location(), targetBlob.location());
            assertThat(sourceBlob.exists()).isFalse();
            assertThat(targetBlob.exists()).isTrue();
            assertThat(targetBlob.read()).isEqualTo("data");

            // rename over existing should fail
            sourceBlob.createOrOverwrite("new data");
            assertThatThrownBy(() -> getFileSystem().renameFile(sourceBlob.location(), targetBlob.location()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(sourceBlob.location().toString())
                    .hasMessageContaining(targetBlob.location().toString());
            assertThat(sourceBlob.exists()).isTrue();
            assertThat(targetBlob.exists()).isTrue();
            assertThat(sourceBlob.read()).isEqualTo("new data");
            assertThat(targetBlob.read()).isEqualTo("data");

            if (isHierarchical()) {
                // todo rename to existing directory name should fail
                // todo rename to existing alias
                try (Closer closer = Closer.create()) {
                    // rename of directory is not allowed
                    createBlob(closer, "a/b");
                    assertThatThrownBy(() -> getFileSystem().renameFile(createLocation("a"), createLocation("b")))
                            .isInstanceOf(IOException.class);
                }
            }
        }
        finally {
            // clean up manually created parent directory
            try {
                getFileSystem().deleteFile(createLocation("renameTarget"));
            }
            catch (IOException ignored) {
            }
        }

        // rename to a file with special characters in the name
        try (TempBlob sourceBlob = randomBlobLocation("renameSource");
                TempBlob targetBlob = randomBlobLocation("renameTarget%25special")) {
            sourceBlob.createOrOverwrite("data");
            getFileSystem().createDirectory(targetBlob.location().parentDirectory());
            getFileSystem().renameFile(sourceBlob.location(), targetBlob.location());
            assertThat(sourceBlob.exists()).isFalse();
            assertThat(targetBlob.exists()).isTrue();
            assertThat(targetBlob.read()).isEqualTo("data");
        }
        finally {
            // clean up manually created parent directory
            try {
                getFileSystem().deleteFile(createLocation("renameTarget%25special"));
            }
            catch (IOException ignored) {
            }
        }
    }

    @Test
    public void testListFiles()
            throws IOException
    {
        testListFiles(isHierarchical());
    }

    @Test
    public void testPreSignedUris()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            Location location = createBlob(closer, "pre_signed");

            if (!supportsPreSignedUri()) {
                assertThatThrownBy(() -> getFileSystem().preSignedUri(location, new Duration(1, SECONDS)))
                        .isInstanceOf(UnsupportedOperationException.class);
                abort("Generating pre-signed URI is not supported");
            }

            Optional<UriLocation> directLocation = getFileSystem()
                    .preSignedUri(location, new Duration(3, SECONDS));

            assertThat(directLocation).isPresent();
            assertThat(retrieveUri(directLocation.get()))
                    .isEqualTo(TEST_BLOB_CONTENT_PREFIX + location);

            // Check if it can be retrieved more than once
            assertThat(retrieveUri(directLocation.get()))
                    .isEqualTo(TEST_BLOB_CONTENT_PREFIX + location);

            // Check if after a timeout the pre-signed URI is no longer valid
            assertEventually(new Duration(5, SECONDS), new Duration(1, SECONDS), () -> assertThatThrownBy(() -> retrieveUri(directLocation.get()))
                    .isInstanceOf(IOException.class));
        }
    }

    private static String retrieveUri(UriLocation uriLocation)
            throws IOException
    {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest request = addHeaders(HttpRequest.newBuilder(), uriLocation.headers())
                    .uri(uriLocation.uri())
                    .GET()
                    .build();

            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    throw new IOException("Failed to retrieve, got response code: %d, body: %s"
                            .formatted(response.statusCode(), response.body()));
                }
                return response.body();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private static HttpRequest.Builder addHeaders(HttpRequest.Builder builder, Map<String, List<String>> headers)
    {
        headers.forEach((headerName, headerValues) -> headerValues.forEach((headerValue) -> builder.header(headerName, headerValue)));
        return builder;
    }

    protected void testListFiles(boolean hierarchicalNamingConstraints)
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            Set<Location> locations = createTestDirectoryStructure(closer, hierarchicalNamingConstraints);

            assertThat(listPath("")).containsExactlyInAnyOrderElementsOf(locations);

            assertThat(listPath("level0")).containsExactlyInAnyOrderElementsOf(locations.stream()
                    .filter(location -> location.toString().startsWith(createLocation("level0/").toString()))
                    .toList());
            assertThat(listPath("level0/")).containsExactlyInAnyOrderElementsOf(locations.stream()
                    .filter(location -> location.toString().startsWith(createLocation("level0/").toString()))
                    .toList());

            assertThat(listPath("level0/level1/")).containsExactlyInAnyOrderElementsOf(locations.stream()
                    .filter(location -> location.toString().startsWith(createLocation("level0/level1/").toString()))
                    .toList());
            assertThat(listPath("level0/level1")).containsExactlyInAnyOrderElementsOf(locations.stream()
                    .filter(location -> location.toString().startsWith(createLocation("level0/level1/").toString()))
                    .toList());

            assertThat(listPath("level0/level1/level2/")).containsExactlyInAnyOrderElementsOf(locations.stream()
                    .filter(location -> location.toString().startsWith(createLocation("level0/level1/level2/").toString()))
                    .toList());
            assertThat(listPath("level0/level1/level2")).containsExactlyInAnyOrderElementsOf(locations.stream()
                    .filter(location -> location.toString().startsWith(createLocation("level0/level1/level2/").toString()))
                    .toList());

            assertThat(listPath("level0/level1/level2/level3")).isEmpty();
            assertThat(listPath("level0/level1/level2/level3/")).isEmpty();

            assertThat(listPath("unknown/")).isEmpty();

            if (isHierarchical()) {
                assertThatThrownBy(() -> listPath("level0-file0"))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(createLocation("level0-file0").toString());
            }
            else {
                assertThat(listPath("level0-file0")).isEmpty();
            }

            if (!hierarchicalNamingConstraints && !normalizesListFilesResult()) {
                // this lists a path in a directory with an empty name
                assertThat(listPath("/")).isEmpty();
            }
        }
    }

    @Test
    public void testDirectoryExists()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            String directoryName = "testDirectoryExistsDir";
            String fileName = "file.csv";

            assertThat(listPath("")).isEmpty();
            assertThat(getFileSystem().directoryExists(getRootLocation())).contains(true);

            if (isHierarchical()) {
                assertThat(getFileSystem().directoryExists(createLocation(directoryName))).contains(false);
                createBlob(closer, createLocation(directoryName).appendPath(fileName).path());
                assertThat(getFileSystem().directoryExists(createLocation(directoryName))).contains(true);
                assertThat(getFileSystem().directoryExists(createLocation(UUID.randomUUID().toString()))).contains(false);
                assertThat(getFileSystem().directoryExists(createLocation(directoryName).appendPath(fileName))).contains(false);
            }
            else {
                assertThat(getFileSystem().directoryExists(createLocation(directoryName))).isEmpty();
                createBlob(closer, createLocation(directoryName).appendPath(fileName).path());
                assertThat(getFileSystem().directoryExists(createLocation(directoryName))).contains(true);
                assertThat(getFileSystem().directoryExists(createLocation(UUID.randomUUID().toString()))).isEmpty();
                assertThat(getFileSystem().directoryExists(createLocation(directoryName).appendPath(fileName))).isEmpty();
            }
        }
    }

    @Test
    public void testFileWithTrailingWhitespace()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            Location location = createBlob(closer, "dir/whitespace ");

            // Verify listing
            assertThat(listPath("dir")).isEqualTo(List.of(location));

            // Verify reading
            TrinoInputFile inputFile = getFileSystem().newInputFile(location);
            assertThat(inputFile.exists()).as("exists").isTrue();
            try (TrinoInputStream inputStream = inputFile.newStream()) {
                byte[] bytes = inputStream.readAllBytes();
                assertThat(bytes).isEqualTo(("test blob content for " + location).getBytes(UTF_8));
            }

            // Verify writing
            byte[] newContents = "bar bar baz new content".getBytes(UTF_8);
            getFileSystem().newOutputFile(location).createOrOverwrite(newContents);

            // Open a new input file with an updated file length. If we read with the old inputFile the cached (wrong) file length would be used.
            // This can break some file system read operations (e.g., TrinoInput.readTail for most filesystems, newStream for caching file systems).
            TrinoInputFile newInputFile = getFileSystem().newInputFile(location);
            try (TrinoInputStream inputStream = newInputFile.newStream()) {
                byte[] bytes = inputStream.readAllBytes();
                assertThat(bytes).isEqualTo(newContents);
            }

            // Verify deleting
            getFileSystem().deleteFile(location);
            assertThat(newInputFile.exists()).as("exists after delete").isFalse();

            // Verify renames
            if (supportsRenameFile()) {
                Location source = createBlob(closer, "dir/another trailing whitespace ");
                Location target = getRootLocation().appendPath("dir/after rename still whitespace ");
                getFileSystem().renameFile(source, target);
                assertThat(getFileSystem().newInputFile(source).exists()).as("source exists after rename").isFalse();
                assertThat(getFileSystem().newInputFile(target).exists()).as("target exists after rename").isTrue();

                try (TrinoInputStream inputStream = getFileSystem().newInputFile(target).newStream()) {
                    byte[] bytes = inputStream.readAllBytes();
                    assertThat(bytes).isEqualTo(("test blob content for " + source).getBytes(UTF_8));
                }

                getFileSystem().deleteFile(target);
                assertThat(getFileSystem().newInputFile(target).exists()).as("target exists after delete").isFalse();
            }
        }
    }

    @Test
    public void testListLexicographicalOrder()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            List<TempBlob> blobs = randomBlobs(closer);

            List<Location> sortedLocations = blobs.stream()
                        .map(TempBlob::location)
                        .sorted(comparing(Location::fileName))
                    .toList();

            assertThat(listPath("")).isEqualTo(sortedLocations);
        }
    }

    @Test
    public void testCreateDirectory()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            getFileSystem().createDirectory(createLocation("level0/level1/level2"));

            Optional<Boolean> expectedExists = isHierarchical() ? Optional.of(true) : Optional.empty();

            assertThat(getFileSystem().directoryExists(createLocation("level0/level1/level2"))).isEqualTo(expectedExists);
            assertThat(getFileSystem().directoryExists(createLocation("level0/level1"))).isEqualTo(expectedExists);
            assertThat(getFileSystem().directoryExists(createLocation("level0"))).isEqualTo(expectedExists);

            Location blob = createBlob(closer, "level0/level1/level2-file");

            if (isHierarchical()) {
                // creating a directory for an existing file location is an error
                assertThatThrownBy(() -> getFileSystem().createDirectory(blob))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(blob.toString());
            }
            else {
                getFileSystem().createDirectory(blob);
            }
            assertThat(readLocation(blob)).isEqualTo(TEST_BLOB_CONTENT_PREFIX + blob);

            // create for existing directory does nothing
            getFileSystem().createDirectory(createLocation("level0"));
            getFileSystem().createDirectory(createLocation("level0/level1"));
            getFileSystem().createDirectory(createLocation("level0/level1/level2"));
        }
    }

    @Test
    public void testRenameDirectory()
            throws IOException
    {
        if (!isHierarchical()) {
            getFileSystem().createDirectory(createLocation("abc"));
            assertThatThrownBy(() -> getFileSystem().renameDirectory(createLocation("source"), createLocation("target")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("does not support directory renames");
            return;
        }

        // rename directory locations cannot be the root of the file system
        assertThatThrownBy(() -> getFileSystem().renameDirectory(getRootLocation(), createLocation("dir")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(getRootLocation().toString());
        assertThatThrownBy(() -> getFileSystem().renameDirectory(createLocation("dir"), getRootLocation()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(getRootLocation().toString());

        try (Closer closer = Closer.create()) {
            getFileSystem().createDirectory(createLocation("level0/level1/level2"));

            Location blob = createBlob(closer, "level0/level1/level2-file");

            assertThat(getFileSystem().directoryExists(createLocation("level0/level1/level2"))).contains(true);
            assertThat(getFileSystem().directoryExists(createLocation("level0/level1"))).contains(true);
            assertThat(getFileSystem().directoryExists(createLocation("level0"))).contains(true);

            // rename interior directory
            getFileSystem().renameDirectory(createLocation("level0/level1"), createLocation("level0/renamed"));

            assertThat(getFileSystem().directoryExists(createLocation("level0/level1"))).contains(false);
            assertThat(getFileSystem().directoryExists(createLocation("level0/level1/level2"))).contains(false);
            assertThat(getFileSystem().directoryExists(createLocation("level0/renamed"))).contains(true);
            assertThat(getFileSystem().directoryExists(createLocation("level0/renamed/level2"))).contains(true);

            assertThat(getFileSystem().newInputFile(blob).exists()).isFalse();

            Location renamedBlob = createLocation("level0/renamed/level2-file");
            assertThat(readLocation(renamedBlob))
                    .isEqualTo(TEST_BLOB_CONTENT_PREFIX + blob);

            // rename to existing directory is an error
            Location blob2 = createBlob(closer, "abc/xyz-file");

            assertThat(getFileSystem().directoryExists(createLocation("abc"))).contains(true);

            assertThatThrownBy(() -> getFileSystem().renameDirectory(createLocation("abc"), createLocation("level0")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(createLocation("abc").toString())
                    .hasMessageContaining(createLocation("level0").toString());

            assertThat(getFileSystem().newInputFile(blob2).exists()).isTrue();
            assertThat(getFileSystem().newInputFile(renamedBlob).exists()).isTrue();
        }
    }

    @Test
    public void testListDirectories()
            throws IOException
    {
        testListDirectories(isHierarchical());
    }

    protected void testListDirectories(boolean hierarchicalNamingConstraints)
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            createTestDirectoryStructure(closer, hierarchicalNamingConstraints);
            createBlob(closer, "level0/level1/level2/level3-file0");
            createBlob(closer, "level0/level1x/level2x-file0");
            createBlob(closer, "other/file");

            assertThat(listDirectories("")).containsOnly(
                    createLocation("level0/"),
                    createLocation("other/"));

            assertThat(listDirectories("level0")).containsOnly(
                    createLocation("level0/level1/"),
                    createLocation("level0/level1x/"));
            assertThat(listDirectories("level0/")).containsOnly(
                    createLocation("level0/level1/"),
                    createLocation("level0/level1x/"));

            assertThat(listDirectories("level0/level1")).containsOnly(
                    createLocation("level0/level1/level2/"));
            assertThat(listDirectories("level0/level1/")).containsOnly(
                    createLocation("level0/level1/level2/"));

            assertThat(listDirectories("level0/level1/level2/level3")).isEmpty();
            assertThat(listDirectories("level0/level1/level2/level3/")).isEmpty();

            assertThat(listDirectories("unknown")).isEmpty();
            assertThat(listDirectories("unknown/")).isEmpty();

            if (isHierarchical()) {
                assertThatThrownBy(() -> listDirectories("level0-file0"))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(createLocation("level0-file0").toString());
            }
            else {
                assertThat(listDirectories("level0-file0")).isEmpty();
            }

            if (!hierarchicalNamingConstraints && !normalizesListFilesResult()) {
                // this lists a path in a directory with an empty name
                assertThat(listDirectories("/")).isEmpty();
            }
        }
    }

    private Set<Location> listDirectories(String path)
            throws IOException
    {
        return getFileSystem().listDirectories(createListingLocation(path));
    }

    private List<Location> listPath(String path)
            throws IOException
    {
        List<Location> locations = new ArrayList<>();
        FileIterator fileIterator = getFileSystem().listFiles(createListingLocation(path));
        while (fileIterator.hasNext()) {
            FileEntry fileEntry = fileIterator.next();
            Location location = fileEntry.location();
            assertThat(fileEntry.length()).isEqualTo(TEST_BLOB_CONTENT_PREFIX.length() + location.toString().length());
            locations.add(location);
        }
        return locations;
    }

    private Location createListingLocation(String path)
    {
        // allow listing a directory with a trailing slash
        if (path.equals("/")) {
            return createLocation("").appendSuffix("/");
        }
        return createLocation(path);
    }

    @Test
    void testFileDoesNotExistUntilClosed()
            throws Exception
    {
        if (!supportsIncompleteWriteNoClobber()) {
            abort("skipped");
        }
        Location location = getRootLocation().appendPath("testFileDoesNotExistUntilClosed-%s".formatted(UUID.randomUUID()));
        getFileSystem().deleteFile(location);
        try (OutputStream out = getFileSystem().newOutputFile(location).create()) {
            assertThat(fileExistsInListing(location)).isFalse();
            assertThat(fileExists(location)).isFalse();
            out.write("test".getBytes(UTF_8));
            assertThat(fileExistsInListing(location)).isFalse();
            assertThat(fileExists(location)).isFalse();
        }
        assertThat(fileExistsInListing(location)).isTrue();
        assertThat(fileExists(location)).isTrue();
        getFileSystem().deleteFile(location);
    }

    @Test
    public void testLargeFileDoesNotExistUntilClosed()
            throws IOException
    {
        if (!supportsIncompleteWriteNoClobber()) {
            abort("skipped");
        }
        Location location = getRootLocation().appendPath("testLargeFileDoesNotExistUntilClosed-%s".formatted(UUID.randomUUID()));
        getFileSystem().deleteFile(location);
        try (OutputStream outputStream = getFileSystem().newOutputFile(location).create()) {
            // Write a 17 MB file to ensure the data is flushed to storage by exceeding the buffer size
            byte[] bytes = getBytes();
            int target = 17 * MEGABYTE;
            int count = 0;
            while (count < target) {
                outputStream.write(bytes);
                count += bytes.length;
                if (count + bytes.length >= target) {
                    assertThat(fileExistsInListing(location)).isFalse();
                    assertThat(fileExists(location)).isFalse();
                }
            }
        }
        assertThat(fileExistsInListing(location)).isTrue();
        assertThat(fileExists(location)).isTrue();
        getFileSystem().deleteFile(location);
    }

    @Test
    void testServerSideEncryptionWithCustomerKey()
            throws IOException
    {
        if (!useServerSideEncryptionWithCustomerKey()) {
            abort("Test is specific to SSE-C");
        }

        Location location = getRootLocation().appendPath("encrypted");

        byte[] data = "this is encrypted data".getBytes(UTF_8);

        // Create encrypted file
        getFileSystem().newOutputFile(location)
                .createOrOverwrite(data);

        if (!(getFileSystem() instanceof EncryptionEnforcingFileSystem encryptionEnforcingFileSystem)) {
            fail("Expected file system to enforce server side encryption");
            return;
        }

        // Try to read it without a key
        assertThatThrownBy(() -> encryptionEnforcingFileSystem.getDelegate().newInputFile(location).newStream().readAllBytes())
                .isInstanceOf(IOException.class);

        assertThat(getFileSystem().newInputFile(location).newStream().readAllBytes())
                .isEqualTo(data);

        getFileSystem().deleteFile(location);
    }

    @SuppressWarnings("ConstantValue")
    private static byte[] getBytes()
    {
        byte[] bytes = new byte[8192];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        assertThat(MEGABYTE % bytes.length).isEqualTo(0);
        return bytes;
    }

    private boolean fileExistsInListing(Location location)
            throws IOException
    {
        FileIterator fileIterator = getFileSystem().listFiles(getRootLocation());
        while (fileIterator.hasNext()) {
            FileEntry fileEntry = fileIterator.next();
            if (fileEntry.location().equals(location)) {
                return true;
            }
        }
        return false;
    }

    private boolean fileExists(Location location)
            throws IOException
    {
        return getFileSystem().newInputFile(location).exists();
    }

    private String readLocation(Location path)
    {
        try (InputStream inputStream = getFileSystem().newInputFile(path).newStream()) {
            return new String(inputStream.readAllBytes(), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected Location createBlob(Closer closer, String path)
    {
        Location location = createLocation(path);
        closer.register(new TempBlob(location)).createOrOverwrite(TEST_BLOB_CONTENT_PREFIX + location.toString());
        return location;
    }

    protected TempBlob randomBlobLocation(String nameHint)
    {
        TempBlob tempBlob = new TempBlob(createLocation("%s/%s".formatted(nameHint, UUID.randomUUID())));
        assertThat(tempBlob.exists()).isFalse();
        return tempBlob;
    }

    private List<TempBlob> randomBlobs(Closer closer)
    {
        char[] chars = new char[] {'a', 'b', 'c', 'd', 'A', 'B', 'C', 'D'};
        ImmutableList.Builder<TempBlob> names = ImmutableList.builder();
        for (int i = 0; i < 100; i++) {
            StringBuilder name = new StringBuilder();
            for (int j = 0; j < 10; j++) {
                name.append(chars[ThreadLocalRandom.current().nextInt(chars.length)]);
            }
            TempBlob tempBlob = new TempBlob(createLocation(name.toString()));
            assertThat(tempBlob.exists()).isFalse();
            tempBlob.createOrOverwrite(TEST_BLOB_CONTENT_PREFIX + tempBlob.location().toString());
            closer.register(tempBlob);
            names.add(tempBlob);
        }
        return names.build();
    }

    private Set<Location> createTestDirectoryStructure(Closer closer, boolean hierarchicalNamingConstraints)
    {
        Set<Location> locations = new HashSet<>();
        if (!hierarchicalNamingConstraints) {
            locations.add(createBlob(closer, "level0"));
        }
        locations.add(createBlob(closer, "level0-file0"));
        locations.add(createBlob(closer, "level0-file1"));
        locations.add(createBlob(closer, "level0-file2"));
        if (!hierarchicalNamingConstraints) {
            locations.add(createBlob(closer, "level0/level1"));
        }
        locations.add(createBlob(closer, "level0/level1-file0"));
        locations.add(createBlob(closer, "level0/level1-file1"));
        locations.add(createBlob(closer, "level0/level1-file2"));
        if (!hierarchicalNamingConstraints) {
            locations.add(createBlob(closer, "level0/level1/level2"));
        }
        locations.add(createBlob(closer, "level0/level1/level2-file0"));
        locations.add(createBlob(closer, "level0/level1/level2-file1"));
        locations.add(createBlob(closer, "level0/level1/level2-file2"));
        return locations;
    }

    protected class TempBlob
            implements Closeable
    {
        private final Location location;
        private final TrinoFileSystem fileSystem;

        public TempBlob(Location location)
        {
            this.location = requireNonNull(location, "location is null");
            fileSystem = getFileSystem();
        }

        public Location location()
        {
            return location;
        }

        public boolean exists()
        {
            try {
                return inputFile().exists();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public TrinoInputFile inputFile()
        {
            return fileSystem.newInputFile(location);
        }

        public TrinoOutputFile outputFile()
        {
            return fileSystem.newOutputFile(location);
        }

        public void createOrOverwrite(String data)
        {
            try {
                outputFile().createOrOverwrite(data.getBytes(UTF_8));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            assertThat(exists()).isTrue();
        }

        public String read()
        {
            return readLocation(location);
        }

        @Override
        public void close()
        {
            try {
                fileSystem.deleteFile(location);
            }
            catch (IOException _) {
            }
        }
    }
}
