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
package io.trino.plugin.iceberg.encryption;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

final class TestFileDecryptionData
{
    @Test
    void testJsonRoundTrip()
    {
        JsonCodec<FileDecryptionData> codec = jsonCodec(FileDecryptionData.class);
        for (FileDecryptionData decryptionData : List.of(
                new ParquetFileDecryptionData(new byte[] {1, 2}, new byte[] {3}),
                new DeletionVectorDecryptionData(new byte[] {1, 2}, new byte[] {3}, 123))) {
            assertThat(codec.fromJson(codec.toJson(decryptionData))).isEqualTo(decryptionData);
        }
    }
}
