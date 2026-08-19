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
package io.trino.plugin.elasticsearch.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.airlift.json.JsonMapperProvider;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.trino.plugin.elasticsearch.client.ElasticsearchClient.hasDocValues;
import static io.trino.plugin.elasticsearch.client.ElasticsearchClient.keywordSubfield;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKeywordSubfield
{
    private static final JsonMapper JSON = new JsonMapperProvider().get();

    @Test
    public void testNoSubfields()
            throws IOException
    {
        JsonNode field = field(
                """
                { "type": "text" }
                """);
        assertThat(keywordSubfield(field, false)).isEmpty();
        assertThat(keywordSubfield(field, true)).isEmpty();
    }

    @Test
    public void testUnboundedKeywordSubfield()
            throws IOException
    {
        // A keyword sub-field without ignore_above indexes all values, so it is safe regardless of the flag
        JsonNode field = field(
                """
                { "type": "text", "fields": { "raw": { "type": "keyword" } } }
                """);
        assertThat(keywordSubfield(field, false)).contains("raw");
        assertThat(keywordSubfield(field, true)).contains("raw");
        assertThat(hasDocValues(field.path("fields").path("raw"))).isTrue();
    }

    @Test
    public void testKeywordSubfieldWithIgnoreAbove()
            throws IOException
    {
        // ignore_above may drop long values from the index, so the sub-field is only used when the operator opts in
        JsonNode field = field(
                """
                { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } }
                """);
        assertThat(keywordSubfield(field, false)).isEmpty();
        assertThat(keywordSubfield(field, true)).contains("keyword");
    }

    @Test
    public void testPrefersUnboundedOverBounded()
            throws IOException
    {
        JsonNode field = field(
                """
                { "type": "text", "fields": {
                    "bounded": { "type": "keyword", "ignore_above": 256 },
                    "raw": { "type": "keyword" } } }
                """);
        assertThat(keywordSubfield(field, false)).contains("raw");
        assertThat(keywordSubfield(field, true)).contains("raw");
    }

    @Test
    public void testNonKeywordSubfieldsIgnored()
            throws IOException
    {
        JsonNode field = field(
                """
                { "type": "text", "fields": { "english": { "type": "text", "analyzer": "english" } } }
                """);
        assertThat(keywordSubfield(field, false)).isEmpty();
        assertThat(keywordSubfield(field, true)).isEmpty();
    }

    @Test
    public void testKeywordSubfieldWithoutDocValuesStillSupportsExactPredicates()
            throws IOException
    {
        JsonNode field = field(
                """
                { "type": "text", "fields": { "raw": { "type": "keyword", "doc_values": false } } }
                """);
        assertThat(keywordSubfield(field, false)).contains("raw");
        assertThat(keywordSubfield(field, true)).contains("raw");
        assertThat(hasDocValues(field.path("fields").path("raw"))).isFalse();
    }

    @Test
    public void testUnboundedExactPredicateSubfieldPreferredEvenWithoutDocValues()
            throws IOException
    {
        JsonNode field = field(
                """
                { "type": "text", "fields": {
                    "unbounded_no_doc_values": { "type": "keyword", "doc_values": false },
                    "bounded": { "type": "keyword", "ignore_above": 256 } } }
                """);
        assertThat(keywordSubfield(field, false)).contains("unbounded_no_doc_values");
        assertThat(keywordSubfield(field, true)).contains("unbounded_no_doc_values");
    }

    @Test
    public void testKeywordSubfieldWithoutIndexOrDocValuesIsIgnored()
            throws IOException
    {
        JsonNode field = field(
                """
                { "type": "text", "fields": { "raw": { "type": "keyword", "index": false, "doc_values": false } } }
                """);
        assertThat(keywordSubfield(field, false)).isEmpty();
        assertThat(keywordSubfield(field, true)).isEmpty();
    }

    @Test
    public void testKeywordSubfieldWithDocValuesButNoIndexIsUsable()
            throws IOException
    {
        JsonNode field = field(
                """
                { "type": "text", "fields": { "raw": { "type": "keyword", "index": false } } }
                """);
        assertThat(keywordSubfield(field, false)).contains("raw");
        assertThat(hasDocValues(field.path("fields").path("raw"))).isTrue();
    }

    private static JsonNode field(String mapping)
            throws IOException
    {
        return JSON.readTree(mapping);
    }
}
