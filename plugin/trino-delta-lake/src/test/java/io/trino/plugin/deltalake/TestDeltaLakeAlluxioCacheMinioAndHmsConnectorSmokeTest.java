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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

/**
 * Delta Lake connector smoke test exercising Hive metastore and MinIO storage with Alluxio caching.
 */
public class TestDeltaLakeAlluxioCacheMinioAndHmsConnectorSmokeTest
        extends TestDeltaLakeMinioAndHmsConnectorSmokeTest
{
    private final List<Path> cacheDirectories = new CopyOnWriteArrayList<>();

    @AfterAll
    final void deleteDirectory()
    {
        for (Path directory : ImmutableList.copyOf(cacheDirectories)) {
            try (Stream<Path> walk = Files.walk(directory)) {
                walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        super.cleanUp();
    }

    @Override
    protected Map<String, String> deltaStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .putAll(super.deltaStorageConfiguration())
                .put("fs.cache.enabled", "true")
                .buildOrThrow();
    }

    @Override
    protected Optional<Map<String, String>> getBlobCacheProperties()
    {
        Path cacheDirectory;
        try {
            cacheDirectory = Files.createTempDirectory("cache");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        cacheDirectories.add(cacheDirectory);
        return Optional.of(ImmutableMap.<String, String>builder()
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .buildOrThrow());
    }
}
