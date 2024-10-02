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
package io.trino.filesystem.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import io.airlift.log.Logging;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.encryption.EncryptionEnforcingFileSystem;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static io.trino.filesystem.s3.S3SseCUtils.encoded;
import static io.trino.filesystem.s3.S3SseCUtils.md5Checksum;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256;

public abstract class AbstractTestS3FileSystem
        extends AbstractTestTrinoFileSystem
{
    protected final EncryptionKey randomEncryptionKey = randomAes256();
    private S3FileSystemFactory fileSystemFactory;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    final void init()
    {
        Logging.initialize();

        initEnvironment();
        fileSystemFactory = createS3FileSystemFactory();
        fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
    }

    @AfterAll
    final void cleanup()
    {
        fileSystem = null;
        fileSystemFactory.destroy();
        fileSystemFactory = null;
    }

    @Override
    protected final boolean isHierarchical()
    {
        return false;
    }

    @Override
    protected final TrinoFileSystem getFileSystem()
    {
        if (useServerSideEncryptionWithCustomerKey()) {
            return new EncryptionEnforcingFileSystem(fileSystem, randomEncryptionKey);
        }
        return fileSystem;
    }

    @Override
    protected final Location getRootLocation()
    {
        return Location.of("s3://%s/".formatted(bucket()));
    }

    @Override
    protected final boolean supportsRenameFile()
    {
        return false;
    }

    @Override
    protected boolean supportsPreSignedUri()
    {
        return true;
    }

    @Override
    protected final void verifyFileSystemIsEmpty()
    {
        try (S3Client client = createS3Client()) {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket())
                    .build();
            assertThat(client.listObjectsV2(request).contents()).isEmpty();
        }
    }

    protected void initEnvironment() {}

    protected abstract String bucket();

    protected abstract S3FileSystemFactory createS3FileSystemFactory();

    protected abstract S3Client createS3Client();

    protected List<FileEntry> toList(FileIterator fileIterator)
            throws IOException
    {
        ImmutableList.Builder<FileEntry> list = ImmutableList.builder();
        while (fileIterator.hasNext()) {
            list.add(fileIterator.next());
        }
        return list.build();
    }

    /**
     * Tests same things as {@link #testFileWithTrailingWhitespace()} but with setup and assertions using {@link S3Client}.
     */
    @Test
    void testFileWithTrailingWhitespaceAgainstNativeClient()
            throws IOException
    {
        try (S3Client s3Client = createS3Client()) {
            String key = "foo/bar with whitespace ";
            byte[] contents = "abc foo bar".getBytes(UTF_8);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucket())
                    .key(key)
                    .applyMutation(builder -> {
                        if (useServerSideEncryptionWithCustomerKey()) {
                            builder.sseCustomerAlgorithm(AES256.toString());
                            builder.sseCustomerKey(encoded(randomEncryptionKey));
                            builder.sseCustomerKeyMD5(md5Checksum(randomEncryptionKey));
                        }
                    })
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(contents.clone()));
            try {
                // Verify listing
                List<FileEntry> listing = toList(getFileSystem().listFiles(getRootLocation().appendPath("foo")));
                assertThat(listing).hasSize(1);
                FileEntry fileEntry = getOnlyElement(listing);
                assertThat(fileEntry.location()).isEqualTo(getRootLocation().appendPath(key));
                assertThat(fileEntry.length()).isEqualTo(contents.length);

                // Verify reading
                TrinoInputFile inputFile = getFileSystem().newInputFile(fileEntry.location());
                assertThat(inputFile.exists()).as("exists").isTrue();
                try (TrinoInputStream inputStream = inputFile.newStream()) {
                    byte[] bytes = ByteStreams.toByteArray(inputStream);
                    assertThat(bytes).isEqualTo(contents);
                }

                // Verify writing
                byte[] newContents = "bar bar baz new content".getBytes(UTF_8);
                getFileSystem().newOutputFile(fileEntry.location()).createOrOverwrite(newContents);
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket(bucket())
                        .key(key)
                        .applyMutation(builder -> {
                            if (useServerSideEncryptionWithCustomerKey()) {
                                builder.sseCustomerAlgorithm(AES256.toString());
                                builder.sseCustomerKey(encoded(randomEncryptionKey));
                                builder.sseCustomerKeyMD5(md5Checksum(randomEncryptionKey));
                            }
                        })
                        .build();

                assertThat(s3Client.getObjectAsBytes(request).asByteArray())
                        .isEqualTo(newContents);

                // Verify deleting
                getFileSystem().deleteFile(fileEntry.location());
                assertThat(inputFile.exists()).as("exists after delete").isFalse();
            }
            finally {
                s3Client.deleteObject(delete -> delete.bucket(bucket()).key(key));
            }
        }
    }

    @Test
    void testExistingDirectoryWithTrailingSlash()
            throws IOException
    {
        try (S3Client s3Client = createS3Client(); Closer closer = Closer.create()) {
            String key = "data/dir/";
            createDirectory(closer, s3Client, key);
            assertThat(getFileSystem().listFiles(getRootLocation()).hasNext()).isFalse();

            Location data = getRootLocation().appendPath("data/");
            assertThat(getFileSystem().listDirectories(getRootLocation())).containsExactly(data);
            assertThat(getFileSystem().listDirectories(data)).containsExactly(data.appendPath("dir/"));

            getFileSystem().deleteDirectory(data);
            assertThat(getFileSystem().listDirectories(getRootLocation())).isEmpty();

            getFileSystem().deleteDirectory(getRootLocation());
            assertThat(getFileSystem().listDirectories(getRootLocation())).isEmpty();
        }
    }

    @Test
    void testDeleteEmptyDirectoryWithDeepHierarchy()
            throws IOException
    {
        try (S3Client s3Client = createS3Client(); Closer closer = Closer.create()) {
            createDirectory(closer, s3Client, "deep/dir");
            createBlob(closer, "deep/dir/file1.txt");
            createBlob(closer, "deep/dir/file2.txt");
            createBlob(closer, "deep/dir/file3.txt");
            createDirectory(closer, s3Client, "deep/dir/dir4");
            createBlob(closer, "deep/dir/dir4/file5.txt");

            assertThat(getFileSystem().listFiles(getRootLocation()).hasNext()).isTrue();

            Location directory = getRootLocation().appendPath("deep/dir/");
            assertThat(getFileSystem().listDirectories(getRootLocation().appendPath("deep"))).containsExactly(directory);
            assertThat(getFileSystem().listDirectories(directory)).containsExactly(getRootLocation().appendPath("deep/dir/dir4/"));

            getFileSystem().deleteDirectory(directory);
            assertThat(getFileSystem().listDirectories(getRootLocation().appendPath("deep"))).isEmpty();
            assertThat(getFileSystem().listDirectories(getRootLocation())).isEmpty();
            assertThat(getFileSystem().listFiles(getRootLocation()).hasNext()).isFalse();
        }
    }

    protected Location createDirectory(Closer closer, S3Client s3Client, String path)
    {
        Location location = createLocation(path);
        closer.register(new TempDirectory(s3Client, path)).create();
        return location;
    }

    protected class TempDirectory
            implements Closeable
    {
        private final S3Client s3Client;
        private final String path;

        public TempDirectory(S3Client s3Client, String path)
        {
            this.s3Client = requireNonNull(s3Client, "s3Client is null");
            String key = requireNonNull(path, "path is null");
            this.path = key.endsWith("/") ? key : key + "/";
        }

        public void create()
        {
            s3Client.putObject(request -> request.bucket(bucket()).key(path), RequestBody.empty());
        }

        @Override
        public void close()
                throws IOException
        {
            s3Client.deleteObject(delete -> delete.bucket(bucket()).key(path));
        }
    }
}
