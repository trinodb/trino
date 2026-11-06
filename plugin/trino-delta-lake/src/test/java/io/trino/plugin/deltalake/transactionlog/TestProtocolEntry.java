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
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestProtocolEntry
{
    private final JsonCodec<ProtocolEntry> codec = jsonCodec(ProtocolEntry.class);

    @Test
    public void testProtocolEntryFromJson()
    {
        @Language("JSON")
        String json = "{\"minReaderVersion\":2,\"minWriterVersion\":5}";
        assertThat(codec.fromJson(json)).isEqualTo(new ProtocolEntry(2, 5, Optional.empty(), Optional.empty()));

        @Language("JSON")
        String jsonWithFeatures = "{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"timestampNTZ\"]}";
        assertThat(codec.fromJson(jsonWithFeatures)).isEqualTo(new ProtocolEntry(3, 7, Optional.of(ImmutableSet.of("deletionVectors")), Optional.of(ImmutableSet.of("timestampNTZ"))));
    }

    @Test
    public void testInvalidProtocolEntryFromJson()
    {
        @Language("JSON")
        String invalidMinReaderVersion = "{\"minReaderVersion\":2,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"]}";
        assertThatThrownBy(() -> codec.fromJson(invalidMinReaderVersion))
                .hasMessageContaining("Invalid JSON string")
                .hasStackTraceContaining("readerFeatures must not exist when minReaderVersion is less than 3");

        // this should fail because when minReaderVersion is 3, readerFeatures must exist in protocol, we allow reading such protocol for backward compatibility
        @Language("JSON")
        String invalidMinWriterVersion = "{\"minReaderVersion\":3,\"minWriterVersion\":6,\"writerFeatures\":[\"timestampNTZ\"]}";
        assertThatThrownBy(() -> codec.fromJson(invalidMinWriterVersion))
                .hasMessageContaining("Invalid JSON string")
                .hasStackTraceContaining("writerFeatures must not exist when minWriterVersion is less than 7");
    }

    @Test
    public void testProtocolEntryToJson()
    {
        assertThat(codec.toJson(new ProtocolEntry(2, 5, Optional.empty(), Optional.empty()))).isEqualTo(
                """
                {
                  "minReaderVersion" : 2,
                  "minWriterVersion" : 5
                }\
                """);

        assertThat(codec.toJson(new ProtocolEntry(3, 7, Optional.of(ImmutableSet.of("deletionVectors")), Optional.of(ImmutableSet.of("timestampNTZ"))))).isEqualTo(
                """
                {
                  "minReaderVersion" : 3,
                  "minWriterVersion" : 7,
                  "readerFeatures" : [ "deletionVectors" ],
                  "writerFeatures" : [ "timestampNTZ" ]
                }\
                """);
    }

    @Test
    public void testBuilderAddsImpliedLegacyFeatures()
    {
        assertThat(ProtocolEntry.builder(new ProtocolEntry(1, 4, Optional.empty(), Optional.empty())).enableTimestampNtz().build())
                .isEqualTo(new ProtocolEntry(
                        3,
                        7,
                        Optional.of(ImmutableSet.of("timestampNtz")),
                        Optional.of(ImmutableSet.of("appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "timestampNtz"))));

        assertThat(ProtocolEntry.builder(new ProtocolEntry(2, 6, Optional.empty(), Optional.empty())).enableDeletionVector().build())
                .isEqualTo(new ProtocolEntry(
                        3,
                        7,
                        Optional.of(ImmutableSet.of("columnMapping", "deletionVectors")),
                        Optional.of(ImmutableSet.of("appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "columnMapping", "identityColumns", "deletionVectors"))));

        // columnMapping is not implied without reader version 2
        assertThat(ProtocolEntry.builder(new ProtocolEntry(1, 5, Optional.empty(), Optional.empty())).enableTimestampNtz().build())
                .isEqualTo(new ProtocolEntry(
                        3,
                        7,
                        Optional.of(ImmutableSet.of("timestampNtz")),
                        Optional.of(ImmutableSet.of("appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "timestampNtz"))));

        // Reader version 2 does not imply columnMapping on its own
        assertThat(ProtocolEntry.builder(new ProtocolEntry(2, 7, Optional.empty(), Optional.of(ImmutableSet.of("appendOnly")))).enableDeletionVector().build())
                .isEqualTo(new ProtocolEntry(3, 7, Optional.of(ImmutableSet.of("deletionVectors")), Optional.of(ImmutableSet.of("appendOnly", "deletionVectors"))));

        // columnMapping listed as a writer feature is added to reader features
        assertThat(ProtocolEntry.builder(new ProtocolEntry(2, 7, Optional.empty(), Optional.of(ImmutableSet.of("columnMapping")))).enableDeletionVector().build())
                .isEqualTo(new ProtocolEntry(3, 7, Optional.of(ImmutableSet.of("columnMapping", "deletionVectors")), Optional.of(ImmutableSet.of("columnMapping", "deletionVectors"))));

        // Legacy versions are kept when the table does not move to table features
        assertThat(ProtocolEntry.builder(new ProtocolEntry(1, 2, Optional.empty(), Optional.empty())).enableChangeDataFeed().build())
                .isEqualTo(new ProtocolEntry(1, 4, Optional.empty(), Optional.empty()));

        // New tables do not list features for the default legacy versions
        assertThat(ProtocolEntry.builder(1, 2).enableTimestampNtz().build())
                .isEqualTo(new ProtocolEntry(3, 7, Optional.of(ImmutableSet.of("timestampNtz")), Optional.of(ImmutableSet.of("timestampNtz"))));
    }
}
