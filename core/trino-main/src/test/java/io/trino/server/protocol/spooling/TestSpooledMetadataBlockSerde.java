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
package io.trino.server.protocol.spooling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.client.spooling.DataAttributes;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.server.protocol.spooling.SpooledMetadataBlockSerde.deserialize;
import static io.trino.server.protocol.spooling.SpooledMetadataBlockSerde.serialize;
import static org.assertj.core.api.Assertions.assertThat;

class TestSpooledMetadataBlockSerde
{
    @Test
    public void testSerializationRoundTrip()
    {
        verifySerialization(utf8Slice("identifier"), Optional.of(URI.create("https://example.com/presigned")), ImmutableMap.of("X-Header", ImmutableList.of("value1", "value2")));
        verifySerialization(utf8Slice("identifier"), Optional.empty(), ImmutableMap.of("X-Header", ImmutableList.of("value1", "value2")));
    }

    public void verifySerialization(Slice identifier, Optional<URI> directUri, Map<String, List<String>> headers)
    {
        verifySerializationRoundTrip(identifier, directUri, headers);
        verifySerializationRoundTripWithNonEmptyPage(identifier, directUri, headers);
    }

    public void verifySerializationRoundTrip(Slice identifier, Optional<URI> directUri, Map<String, List<String>> headers)
    {
        SpooledMetadataBlock metadata = new SpooledMetadataBlock.Spooled(createDataAttributes(10, 1200), identifier, directUri, headers);
        assertThat(List.of(metadata)).isEqualTo(deserialize(serialize(metadata)));
    }

    private void verifySerializationRoundTripWithNonEmptyPage(Slice identifier, Optional<URI> directUri, Map<String, List<String>> headers)
    {
        SpooledMetadataBlock metadata = new SpooledMetadataBlock.Spooled(createDataAttributes(10, 1100), identifier, directUri, headers);
        assertThat(List.of(metadata)).isEqualTo(deserialize(serialize(metadata)));
    }

    private static DataAttributes createDataAttributes(long rows, int segmentSize)
    {
        return DataAttributes.builder()
                .set(ROWS_COUNT, rows)
                .set(SEGMENT_SIZE, segmentSize)
                .build();
    }
}
