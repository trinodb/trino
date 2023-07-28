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
package io.trino.client;

import org.junit.jupiter.api.Test;

import static io.trino.client.QueryDataEncodings.singleEncoding;
import static io.trino.client.QueryDataReference.EXTERNAL;
import static io.trino.client.QueryDataReference.INLINE;
import static io.trino.client.QueryDataSerialization.JSON;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryDataEncodings
{
    @Test
    public void testSingleDataFormat()
    {
        assertRoundTrip(singleEncoding(INLINE, JSON), "inline-json");
        assertRoundTrip(singleEncoding(EXTERNAL, JSON), "external-json");
    }

    @Test
    public void testMultipleSelectors()
    {
        assertRoundTrip(singleEncoding(INLINE, JSON).and(singleEncoding(EXTERNAL, JSON)), "inline-json;external-json");
    }

    public void assertRoundTrip(QueryDataEncodings selector, String expectedSelector)
    {
        assertThat(selector.matchesAny(selector)).isTrue();
        assertThat(selector.toString()).isEqualTo(expectedSelector);
        assertThat(QueryDataEncodings.parseEncodings(selector.toString())).isEqualTo(selector);
    }
}
