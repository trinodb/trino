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
package io.trino.spooling.filesystem;

import io.azam.ulidj.ULID;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestPartitionedFileSystemLayout
{
    private static final byte[] STATIC_ENTROPY = "notsorandombytes".getBytes(UTF_8);
    private static final FileSystemLayout LAYOUT = new PartitionedFileSystemLayout(new PartitionedLayoutConfig().setPartitions(7));
    private static final Location ROOT_LOCATION = Location.of("memory://root/");

    @Test
    public void testStorageLocation()
    {
        FileSystemSpooledSegmentHandle handle = new FileSystemSpooledSegmentHandle("json", ULID.generateBinary(213700331, STATIC_ENTROPY), Optional.empty());

        assertThat(handle.identifier()).isEqualTo("00006BSKQBDSQQ8WVFE9GPWS3F");

        Location segmentLocation = LAYOUT.location(ROOT_LOCATION, handle);

        assertThat(segmentLocation).isEqualTo(ROOT_LOCATION
                .appendPath("600-spooled")
                .appendPath("00006BSKQBDSQQ8WVFE9GPWS3F.json"));

        assertThat(segmentLocation.fileName()).isEqualTo("00006BSKQBDSQQ8WVFE9GPWS3F.json");
        assertThat(LAYOUT.getExpiration(segmentLocation)).hasValue(Instant.ofEpochMilli(213700331));
    }

    @Test
    public void testSearchPaths()
    {
        assertThat(LAYOUT.searchPaths(ROOT_LOCATION)).containsOnly(
                ROOT_LOCATION.appendPath("000-spooled"),
                ROOT_LOCATION.appendPath("100-spooled"),
                ROOT_LOCATION.appendPath("200-spooled"),
                ROOT_LOCATION.appendPath("300-spooled"),
                ROOT_LOCATION.appendPath("400-spooled"),
                ROOT_LOCATION.appendPath("500-spooled"),
                ROOT_LOCATION.appendPath("600-spooled"));
    }

    @Test
    public void testExpirationForNonSegment()
    {
        Location fileLocation = ROOT_LOCATION.appendPath("not_a_segment.json");
        assertThat(LAYOUT.getExpiration(fileLocation)).isEmpty();
    }
}
