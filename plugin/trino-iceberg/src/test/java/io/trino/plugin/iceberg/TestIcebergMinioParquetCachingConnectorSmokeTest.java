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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.trino.filesystem.Location;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;

public class TestIcebergMinioParquetCachingConnectorSmokeTest
        extends BaseIcebergMinioConnectorSmokeTest
{
    private final Path cacheDirectory;
    private final Closer closer = Closer.create();

    TestIcebergMinioParquetCachingConnectorSmokeTest()
            throws IOException
    {
        super(FileFormat.PARQUET);
        cacheDirectory = Files.createTempDirectory("cache");
        closer.register(() -> deleteRecursively(cacheDirectory, ALLOW_INSECURE));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closer.close();
    }

    @Override
    public Map<String, String> getAdditionalIcebergProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.cache.enabled", "true")
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .buildOrThrow();
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
    }
}
