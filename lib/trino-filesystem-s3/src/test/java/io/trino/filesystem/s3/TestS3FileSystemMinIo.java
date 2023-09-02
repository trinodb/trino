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

import io.trino.filesystem.s3.S3FileSystemTestingEnvironment.S3FileSystemTestingEnvironmentMinIo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3FileSystemMinIo
        extends AbstractTestS3FileSystem
{
    private S3FileSystemTestingEnvironmentMinIo testingEnvironment;

    @BeforeAll
    public void setup()
    {
        testingEnvironment = new S3FileSystemTestingEnvironmentMinIo();
    }

    @AfterAll
    public void cleanup()
    {
        if (testingEnvironment != null) {
            testingEnvironment.close();
            testingEnvironment = null;
        }
    }

    @Override
    protected S3FileSystemTestingEnvironment s3TestingEnvironment()
    {
        return testingEnvironment;
    }

    @Test
    @Override
    public void testPaths()
    {
        assertThatThrownBy(super::testPaths)
                .isInstanceOf(IOException.class)
                // MinIO does not support object keys with directory navigation ("/./" or "/../") or with double slashes ("//")
                .hasMessage("S3 HEAD request failed for file: s3://" + testingEnvironment.bucket() + "/test/.././/file");
    }

    @Test
    @Override
    public void testListFiles()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testListFiles(true);
    }

    @Test
    @Override
    public void testDeleteDirectory()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testDeleteDirectory(true);
    }
}
