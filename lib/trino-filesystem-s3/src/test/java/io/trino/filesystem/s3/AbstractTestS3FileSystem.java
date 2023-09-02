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
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.AbstractTrinoFileSystemTestingEnvironment;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestS3FileSystem
        extends AbstractTestTrinoFileSystem
{
    @Override
    protected AbstractTrinoFileSystemTestingEnvironment testingEnvironment()
    {
        return s3TestingEnvironment();
    }

    protected abstract S3FileSystemTestingEnvironment s3TestingEnvironment();

    /**
     * Tests same things as {@link #testFileWithTrailingWhitespace()} but with setup and assertions using {@link S3Client}.
     */
    @Test
    public void testFileWithTrailingWhitespaceAgainstNativeClient()
            throws IOException
    {
        try (S3Client s3Client = s3TestingEnvironment().createS3Client()) {
            String key = "foo/bar with whitespace ";
            byte[] contents = "abc foo bar".getBytes(UTF_8);
            s3Client.putObject(
                    request -> request.bucket(s3TestingEnvironment().bucket()).key(key),
                    RequestBody.fromBytes(contents.clone()));
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
                try (OutputStream outputStream = getFileSystem().newOutputFile(fileEntry.location()).createOrOverwrite()) {
                    outputStream.write(newContents.clone());
                }
                assertThat(s3Client.getObjectAsBytes(request -> request.bucket(s3TestingEnvironment().bucket()).key(key)).asByteArray())
                        .isEqualTo(newContents);

                // Verify deleting
                getFileSystem().deleteFile(fileEntry.location());
                assertThat(inputFile.exists()).as("exists after delete").isFalse();
            }
            finally {
                s3Client.deleteObject(delete -> delete.bucket(s3TestingEnvironment().bucket()).key(key));
            }
        }
    }

    protected List<FileEntry> toList(FileIterator fileIterator)
            throws IOException
    {
        ImmutableList.Builder<FileEntry> list = ImmutableList.builder();
        while (fileIterator.hasNext()) {
            list.add(fileIterator.next());
        }
        return list.build();
    }
}
