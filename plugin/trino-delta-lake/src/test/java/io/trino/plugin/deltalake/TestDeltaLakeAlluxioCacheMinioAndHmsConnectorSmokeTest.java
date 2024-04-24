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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Delta Lake connector smoke test exercising Hive metastore and MinIO storage with Alluxio caching.
 */
public class TestDeltaLakeAlluxioCacheMinioAndHmsConnectorSmokeTest
        extends TestDeltaLakeMinioAndHmsConnectorSmokeTest
{
    private Path cacheDirectory;

    @BeforeAll
    @Override
    public void init()
            throws Exception
    {
        cacheDirectory = Files.createTempDirectory("cache");
        super.init();
    }

    @AfterAll
    @Override
    public void cleanUp()
    {
        try (Stream<Path> walk = Files.walk(cacheDirectory)) {
            Iterator<Path> iterator = walk.sorted(Comparator.reverseOrder()).iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                Files.delete(path);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        super.cleanUp();
    }

    @Override
    protected Map<String, String> deltaStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .putAll(super.deltaStorageConfiguration())
                .put("fs.cache.enabled", "true")
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .buildOrThrow();
    }
}
