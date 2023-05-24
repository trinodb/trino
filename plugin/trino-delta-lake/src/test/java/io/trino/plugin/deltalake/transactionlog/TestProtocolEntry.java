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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestProtocolEntry
{
    private final JsonCodec<ProtocolEntry> codec = JsonCodec.jsonCodec(ProtocolEntry.class);

    @Test
    public void testProtocolEntryFromJson()
    {
        @Language("JSON")
        String json = "{\"minReaderVersion\":2,\"minWriterVersion\":5}";
        assertEquals(
                codec.fromJson(json),
                new ProtocolEntry(2, 5, Optional.empty(), Optional.empty()));

        @Language("JSON")
        String jsonWithFeatures = "{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"timestampNTZ\"]}";
        assertEquals(
                codec.fromJson(jsonWithFeatures),
                new ProtocolEntry(3, 7, Optional.of(ImmutableSet.of("deletionVectors")), Optional.of(ImmutableSet.of("timestampNTZ"))));
    }

    @Test
    public void testInvalidProtocolEntryFromJson()
    {
        @Language("JSON")
        String invalidMinReaderVersion = "{\"minReaderVersion\":2,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"]}";
        assertThatThrownBy(() -> codec.fromJson(invalidMinReaderVersion))
                .hasMessageContaining("Invalid JSON string")
                .hasStackTraceContaining("readerFeatures must not exist when minReaderVersion is less than 3");

        @Language("JSON")
        String invalidMinWriterVersion = "{\"minReaderVersion\":3,\"minWriterVersion\":6,\"writerFeatures\":[\"timestampNTZ\"]}";
        assertThatThrownBy(() -> codec.fromJson(invalidMinWriterVersion))
                .hasMessageContaining("Invalid JSON string")
                .hasStackTraceContaining("writerFeatures must not exist when minWriterVersion is less than 7");
    }

    @Test
    public void testProtocolEntryToJson()
    {
        assertEquals(
                codec.toJson(new ProtocolEntry(2, 5, Optional.empty(), Optional.empty())),
                """
                {
                  "minReaderVersion" : 2,
                  "minWriterVersion" : 5
                }""");

        assertEquals(
                codec.toJson(new ProtocolEntry(3, 7, Optional.of(ImmutableSet.of("deletionVectors")), Optional.of(ImmutableSet.of("timestampNTZ")))),
                """
                {
                  "minReaderVersion" : 3,
                  "minWriterVersion" : 7,
                  "readerFeatures" : [ "deletionVectors" ],
                  "writerFeatures" : [ "timestampNTZ" ]
                }""");
    }
}
