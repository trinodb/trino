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

import io.airlift.log.Logging;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestS3FileSystem
        extends AbstractTestTrinoFileSystem
{
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
        return fileSystem;
    }

    @Override
    protected final Location getRootLocation()
    {
        return Location.of("s3://%s/".formatted(bucket()));
    }

    @Override
    protected final boolean supportsCreateWithoutOverwrite()
    {
        return false;
    }

    @Override
    protected final boolean supportsRenameFile()
    {
        return false;
    }

    @Override
    protected final boolean deleteFileFailsIfNotExists()
    {
        return false;
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
}
