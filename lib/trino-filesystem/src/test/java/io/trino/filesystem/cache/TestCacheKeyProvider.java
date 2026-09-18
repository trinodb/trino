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
package io.trino.filesystem.cache;

import io.trino.filesystem.Location;
import io.trino.spi.cache.CacheKey;
import org.junit.jupiter.api.Test;

import static io.trino.filesystem.cache.CacheKeyProvider.locationKey;
import static org.assertj.core.api.Assertions.assertThat;

class TestCacheKeyProvider
{
    @Test
    void testFileKeyStartsWithContainingDirectoryKey()
    {
        CacheKey file = locationKey(Location.of("s3://bucket/warehouse/schema/table/metadata/00001.metadata.json"));
        assertThat(file.startsWith(locationKey(Location.of("s3://bucket/warehouse/schema/table/metadata")))).isTrue();
        assertThat(file.startsWith(locationKey(Location.of("s3://bucket/warehouse/schema/table")))).isTrue();
        assertThat(file.startsWith(locationKey(Location.of("s3://bucket/warehouse")))).isTrue();
    }

    @Test
    void testTrailingSlashDoesNotAffectDirectoryKey()
    {
        assertThat(locationKey(Location.of("s3://bucket/warehouse/metadata/")))
                .isEqualTo(locationKey(Location.of("s3://bucket/warehouse/metadata")));
    }

    @Test
    void testInteriorEmptySegmentsDoNotCollide()
    {
        // a//b and a/b are distinct objects on object stores and must not share a key
        assertThat(locationKey(Location.of("s3://bucket/a//b")))
                .isNotEqualTo(locationKey(Location.of("s3://bucket/a/b")));
    }

    @Test
    void testDifferentAuthoritiesDoNotCollide()
    {
        assertThat(locationKey(Location.of("s3://bucket-a/x/y")))
                .isNotEqualTo(locationKey(Location.of("s3://bucket-b/x/y")));
        assertThat(locationKey(Location.of("s3://bucket-a/x/y")).startsWith(locationKey(Location.of("s3://bucket-b/x")))).isFalse();
    }
}
