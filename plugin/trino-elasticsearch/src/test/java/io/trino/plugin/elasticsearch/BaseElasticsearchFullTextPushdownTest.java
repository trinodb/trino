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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.planner.plan.FilterNode;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance contract for full-text predicate pushdown shared by the Elasticsearch 7 and 8 connector suites.
 *
 * <p>UNSAFE means that a valid Elasticsearch translation is authoritative: the predicate is fully pushed and Trino
 * does not keep an exact SQL residual merely to preserve SQL string semantics. Unsupported translations still remain
 * in Trino. These tests intentionally exercise cases where Elasticsearch analysis semantics differ from SQL.</p>
 */
public abstract class BaseElasticsearchFullTextPushdownTest
        extends BaseElasticsearchConnectorTest
{
    private final RestClient client;

    protected BaseElasticsearchFullTextPushdownTest(ElasticsearchServer server)
    {
        super(server);
        this.client = server.getClient();
    }

    @Override
    @Test
    public void testRegexpLikeIsNotPushedDown()
            throws IOException
    {
        String indexName = "regexp_like_pushdown_modes_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                  "properties": {
                    "value": { "type": "keyword" },
                    "id": { "type": "keyword" }
                  }
                }
                """;
        createIndex(indexName, properties);
        try {
            index(indexName, ImmutableMap.of("value", "123", "id", "1"));
            index(indexName, ImmutableMap.of("value", "abc", "id", "2"));
            index(indexName, ImmutableMap.of("value", "foo", "id", "3"));

            String catalogName = getSession().getCatalog().orElseThrow();
            Session disabled = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "DISABLED")
                    .build();
            Session safe = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "SAFE")
                    .build();
            Session unsafe = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "UNSAFE")
                    .build();

            // DISABLED never translates regexp_like.
            assertThat(query(disabled, "SELECT id FROM " + indexName + " WHERE regexp_like(value, '123')"))
                    .matches("VALUES VARCHAR '1'")
                    .isNotFullyPushedDown(FilterNode.class);

            // SAFE may use an exact translation as a candidate pre-filter, but Trino remains authoritative.
            assertThat(query(safe, "SELECT id FROM " + indexName + " WHERE regexp_like(value, '123')"))
                    .matches("VALUES VARCHAR '1'")
                    .isNotFullyPushedDown(FilterNode.class);

            // UNSAFE trusts a valid exact translation and removes the residual.
            assertThat(query(unsafe, "SELECT id FROM " + indexName + " WHERE regexp_like(value, '123')"))
                    .matches("VALUES VARCHAR '1'")
                    .isFullyPushedDown();

            // Non-capturing groups are classified as APPROXIMATE by the translator. SAFE must not use that
            // approximation as a candidate pre-filter, while UNSAFE accepts it as authoritative.
            assertThat(query(safe, "SELECT id FROM " + indexName + " WHERE regexp_like(value, '(?:foo|bar)')"))
                    .matches("VALUES VARCHAR '3'")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(unsafe, "SELECT id FROM " + indexName + " WHERE regexp_like(value, '(?:foo|bar)')"))
                    .matches("VALUES VARCHAR '3'")
                    .isFullyPushedDown();

            // Unsupported syntax has no valid remote translation. UNSAFE must not manufacture a Lucene regexp for it.
            assertThat(query(unsafe, "SELECT id FROM " + indexName + " WHERE regexp_like(value, '(?=123)123')"))
                    .matches("VALUES VARCHAR '1'")
                    .isNotFullyPushedDown(FilterNode.class);
        }
        finally {
            deleteIndex(indexName);
        }
    }

    @Override
    @Test
    public void testLike()
            throws IOException
    {
        String indexName = "like_test";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "keyword_column":   { "type": "keyword" },
                    "text_column":      { "type": "text" }
                  }
                }
                """;

        createIndex(indexName, mappings);
        try {
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("keyword_column", "so.me tex\\t")
                    .put("text_column", "so.me tex\\t")
                    .buildOrThrow());

            // Add another document to make sure '.' is escaped and not treated as any character
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("keyword_column", "soome tex\\t")
                    .put("text_column", "soome tex\\t")
                    .buildOrThrow());

            // Add another document to make sure '%' can be escaped and not treated as any character
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("keyword_column", "soome%text")
                    .put("text_column", "soome%text")
                    .buildOrThrow());

            // A phrase that is unambiguous for the analyzed multi-token contains assertion below
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("keyword_column", "unrelated")
                    .put("text_column", "prefix alpha beta suffix")
                    .buildOrThrow());

            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("keyword_column", "中文")
                    .put("text_column", "中文")
                    .buildOrThrow());
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("keyword_column", "こんにちは")
                    .put("text_column", "こんにちは")
                    .buildOrThrow());
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("keyword_column", "안녕하세요")
                    .put("text_column", "안녕하세요")
                    .buildOrThrow());
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("keyword_column", "Привет")
                    .put("text_column", "Привет")
                    .buildOrThrow());

            assertThat(query(
                    """
                    SELECT keyword_column
                    FROM like_test
                    WHERE keyword_column
                    LIKE 's_.m%ex\\t'
                    """))
                    .matches("VALUES VARCHAR 'so.me tex\\t'")
                    .isFullyPushedDown();

            assertThat(query("SELECT keyword_column FROM " + indexName + " WHERE starts_with(keyword_column, 'so.me')"))
                    .matches("VALUES VARCHAR 'so.me tex\\t'")
                    .isFullyPushedDown();

            assertThat(query("SELECT keyword_column FROM " + indexName + " WHERE substr(keyword_column, 1, 2) = '中文'"))
                    .matches("VALUES VARCHAR '中文'")
                    .isFullyPushedDown();

            String catalogName = getSession().getCatalog().orElseThrow();
            Session unsafeFullText = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "UNSAFE")
                    .build();
            Session safeFullText = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "SAFE")
                    .build();

            // UNSAFE trusts Elasticsearch match_phrase semantics for analyzed text equality.
            assertThat(query(unsafeFullText, "SELECT text_column FROM " + indexName + " WHERE text_column = 'soome%text'"))
                    .matches("VALUES VARCHAR 'soome%text'")
                    .isFullyPushedDown();

            // SAFE keeps the exact equality predicate as a Trino residual.
            assertThat(query(safeFullText, "SELECT text_column FROM " + indexName + " WHERE text_column = 'soome%text'"))
                    .matches("VALUES VARCHAR 'soome%text'")
                    .isNotFullyPushedDown(FilterNode.class);

            // Contains-literal LIKE on analyzed text is rewritten to match_phrase, not a term-level regexp.
            assertThat(query(unsafeFullText, "SELECT text_column FROM " + indexName + " WHERE text_column LIKE '%soome%'"))
                    .isFullyPushedDown();

            // A multi-token contains literal is analyzed as one phrase. Whitespace is not a fallback boundary.
            assertThat(query(unsafeFullText, "SELECT text_column FROM " + indexName + " WHERE text_column LIKE '%alpha beta%'"))
                    .matches("VALUES VARCHAR 'prefix alpha beta suffix'")
                    .isFullyPushedDown();

            assertThat(query(unsafeFullText, "SELECT text_column FROM " + indexName + " WHERE regexp_like(text_column, 'soome')"))
                    .isFullyPushedDown();
            assertThat(query(safeFullText, "SELECT text_column FROM " + indexName + " WHERE regexp_like(text_column, 'soome')"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(unsafeFullText, "SELECT text_column FROM " + indexName + " WHERE regexp_like(text_column, '\\d+')"))
                    .isFullyPushedDown();

            // Multi-token prefix is authoritative in UNSAFE and therefore has no Trino residual.
            assertThat(query(unsafeFullText, "SELECT text_column FROM " + indexName + " WHERE text_column LIKE 'soome te%'"))
                    .skipResultsCorrectnessCheckForPushdown()
                    .isFullyPushedDown();

            // An independent predicate on the same column must remain a Trino residual. The LIKE-derived range may be
            // removed only when the prefix LIKE is the sole predicate contributing semantics for this column.
            assertThat(query(unsafeFullText, "SELECT text_column FROM " + indexName + " WHERE text_column LIKE 'soome te%' AND text_column > 'soome tea'"))
                    .matches("VALUES VARCHAR 'soome tex\\t'")
                    .isNotFullyPushedDown(FilterNode.class);

            assertThat(query(
                    """
                    SELECT text_column
                    FROM like_test
                    WHERE text_column
                    LIKE 's_.m%ex\\t'
                    """))
                    .matches("VALUES VARCHAR 'so.me tex\\t'");

            assertThat(query("SELECT text_column FROM " + indexName + " WHERE keyword_column LIKE 'soome$%%' ESCAPE '$'"))
                    .matches("VALUES VARCHAR 'soome%text'")
                    .isFullyPushedDown();
            assertThat(query("SELECT text_column FROM " + indexName + " WHERE keyword_column LIKE '中%'"))
                    .matches("VALUES VARCHAR '中文'")
                    .isFullyPushedDown();
            assertThat(query("SELECT text_column FROM " + indexName + " WHERE keyword_column LIKE 'こんに%'"))
                    .matches("VALUES VARCHAR 'こんにちは'")
                    .isFullyPushedDown();
            assertThat(query("SELECT text_column FROM " + indexName + " WHERE keyword_column LIKE '안녕하%'"))
                    .matches("VALUES VARCHAR '안녕하세요'")
                    .isFullyPushedDown();
            assertThat(query("SELECT text_column FROM " + indexName + " WHERE keyword_column LIKE 'При%'"))
                    .matches("VALUES VARCHAR 'Привет'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(indexName);
        }
    }

    @Test
    public void testUnsafeLikePushdownUsesTextAnalyzer()
            throws IOException
    {
        String indexName = "unsafe_like_analyzer_" + randomNameSuffix();
        @Language("JSON")
        String body =
                """
                {
                  "settings": {
                    "analysis": {
                      "analyzer": {
                        "folded_text": {
                          "type": "custom",
                          "tokenizer": "standard",
                          "filter": ["lowercase", "asciifolding"]
                        }
                      }
                    }
                  },
                  "mappings": {
                    "properties": {
                      "name": { "type": "text", "analyzer": "folded_text" },
                      "id": { "type": "keyword" }
                    }
                  }
                }
                """;

        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(body);
        client.performRequest(request);
        try {
            index(indexName, ImmutableMap.of("name", "NGÔ VĂN", "id", "1"));
            index(indexName, ImmutableMap.of("name", "TRẦN VĂN", "id", "2"));

            String catalogName = getSession().getCatalog().orElseThrow();
            Session unsafe = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "UNSAFE")
                    .build();

            // The analyzer indexes folded lowercase terms ("ngo", "van"). A term-level regexp for "ngô" would miss;
            // match_phrase analyzes the literal with the field analyzer and therefore finds the source row. UNSAFE
            // intentionally does not promise equivalence to local SQL LIKE semantics.
            assertThat(query(unsafe, "SELECT id FROM " + indexName + " WHERE name LIKE '%ngô%'"))
                    .matches("VALUES VARCHAR '1'")
                    .skipResultsCorrectnessCheckForPushdown()
                    .isFullyPushedDown();

            assertThat(query(unsafe, "SELECT id FROM " + indexName + " WHERE name LIKE '%ngô văn%'"))
                    .matches("VALUES VARCHAR '1'")
                    .skipResultsCorrectnessCheckForPushdown()
                    .isFullyPushedDown();

            // Prefix stays on match_phrase_prefix and is also analyzed by the field analyzer.
            assertThat(query(unsafe, "SELECT id FROM " + indexName + " WHERE name LIKE 'ngô văn%'"))
                    .matches("VALUES VARCHAR '1'")
                    .skipResultsCorrectnessCheckForPushdown()
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(indexName);
        }
    }

    private void createIndex(String indexName, @Language("JSON") String properties)
            throws IOException
    {
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity("{\"mappings\": " + properties + "}");
        client.performRequest(request);
    }

    private void index(String index, Map<String, Object> document)
            throws IOException
    {
        String json = new JsonMapper().writeValueAsString(document);
        Request request = new Request("PUT", format("/%s/_doc/%s?refresh", index, System.nanoTime()));
        request.setJsonEntity(json);
        client.performRequest(request);
    }

    private void deleteIndex(String indexName)
            throws IOException
    {
        client.performRequest(new Request("DELETE", "/" + indexName));
    }
}
