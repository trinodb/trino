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

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestCheckpointMetadataEntry
{
    private final JsonCodec<CheckpointMetadataEntry> codec = JsonCodec.jsonCodec(CheckpointMetadataEntry.class);

    @Test
    void testCheckpointMetadataEntry()
    {
        @Language("JSON")
        String json = "{\"version\":5,\"tags\":{\"sidecarNumActions\":\"1\",\"sidecarSizeInBytes\":\"20965\",\"numOfAddFiles\":\"1\",\"sidecarFileSchema\":\"\"}}";
        assertThat(codec.fromJson(json)).isEqualTo(new CheckpointMetadataEntry(
                5,
                Optional.of(ImmutableMap.of(
                        "sidecarNumActions", "1",
                        "sidecarSizeInBytes", "20965",
                        "numOfAddFiles", "1",
                        "sidecarFileSchema", ""))));

        @Language("JSON")
        String jsonWithVersionZero = "{\"version\":0,\"tags\":{\"sidecarNumActions\":\"1\",\"sidecarSizeInBytes\":\"20965\",\"numOfAddFiles\":\"1\",\"sidecarFileSchema\":\"\"}}";
        assertThat(codec.fromJson(jsonWithVersionZero)).isEqualTo(new CheckpointMetadataEntry(
                0,
                Optional.of(ImmutableMap.of(
                        "sidecarNumActions", "1",
                        "sidecarSizeInBytes", "20965",
                        "numOfAddFiles", "1",
                        "sidecarFileSchema", ""))));
    }

    @Test
    void testInvalidCheckpointMetadataEntry()
    {
        @Language("JSON")
        String jsonWithNegativeVersion = "{\"version\":-1,\"tags\":{\"sidecarNumActions\":\"1\",\"sidecarSizeInBytes\":\"20965\",\"numOfAddFiles\":\"1\",\"sidecarFileSchema\":\"\"}}";
        assertThatThrownBy(() -> codec.fromJson(jsonWithNegativeVersion))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid JSON string for");

        @Language("JSON")
        String jsonWithoutTags = "{\"version\":-1}";
        assertThatThrownBy(() -> codec.fromJson(jsonWithoutTags))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid JSON string for");
    }

    @Test
    void testCheckpointMetadataEntryToJson()
    {
        assertThat(codec.toJson(new CheckpointMetadataEntry(
                100,
                Optional.of(ImmutableMap.of(
                        "sidecarNumActions", "1",
                        "sidecarSizeInBytes", "20965",
                        "numOfAddFiles", "1",
                        "sidecarFileSchema", "")))))
                .isEqualTo("""
                        {
                          "version" : 100,
                          "tags" : {
                            "sidecarNumActions" : "1",
                            "sidecarSizeInBytes" : "20965",
                            "numOfAddFiles" : "1",
                            "sidecarFileSchema" : ""
                          }
                        }""");
    }
}
