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
package io.trino.filesystem.cache.local;

import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

final class TestCacheFileLayout
{
    @Test
    void testLayoutUsesStableHashes(@TempDir Path cacheRoot)
    {
        CacheFileLayout layout = new CacheFileLayout(cacheRoot);
        Location location = Location.of("s3://bucket/table/file.orc");
        String cacheKey = "s3://bucket/table/file.orc#version";

        String locationHash = CacheFileLayout.hash(location.toString());
        String fileHash = CacheFileLayout.hash(cacheKey);

        CacheFileLayout.CacheFile cacheFile = layout.cacheFile(location, cacheKey);

        assertThat(cacheFile.fileHash()).isEqualTo(fileHash);
        assertThat(cacheFile.fileGroupPath()).isEqualTo(cacheRoot.toAbsolutePath().normalize()
                .resolve("v1")
                .resolve("data")
                .resolve(locationHash.substring(0, 2))
                .resolve(locationHash.substring(2, 4))
                .resolve(locationHash)
                .resolve(fileHash));
        assertThat(cacheFile.pagePath(42)).isEqualTo(cacheFile.fileGroupPath().resolve("000000000000002a.cache"));
    }
}
