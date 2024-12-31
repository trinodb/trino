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
package io.trino.plugin.paimon;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link PaimonSplit}.
 */
public class PaimonSplitTest
{
    private final JsonCodec<PaimonSplit> codec = JsonCodec.jsonCodec(PaimonSplit.class);

    @Test
    public void testJsonRoundTrip()
            throws Exception
    {
        byte[] serializedTable = PaimonTestUtils.getSerializedTable();
        PaimonSplit expected = new PaimonSplit(Arrays.toString(serializedTable), 0.1);
        String json = codec.toJson(expected);
        PaimonSplit actual = codec.fromJson(json);
        assertThat(actual.getSplitSerialized()).isEqualTo(expected.getSplitSerialized());
    }
}
