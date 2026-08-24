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
import com.google.common.collect.ImmutableList;
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
 * P1.2 acceptance contract for permanent document-scope predicate composition.
 *
 * <p>These tests intentionally keep document-scope array conjunction separate from P1.1 same-element
 * {@code any_match} semantics. Partial OR never pushes only a subset of branches, SAFE never uses an unproven lossy
 * analyzed-text candidate, and UNSAFE may compose explicitly approximate full-text predicates.</p>
 */
public abstract class BaseElasticsearchPredicateCompositionTest
        extends BaseElasticsearchAnyMatchPushdownTest
{
    private final RestClient client;

    protected BaseElasticsearchPredicateCompositionTest(ElasticsearchServer server)
    {
        super(server);
        this.client = server.getClient();
    }

    @Test
    public void testPermanentPredicateComposition()
            throws IOException
    {
        String indexName = "p1_predicate_composition_" + randomNameSuffix();
        @Language("JSON")
        String mapping =
                """
                {
                  "_meta": {
                    "trino": {
                      "numbers": { "isArray": true },
                      "tags": { "isArray": true }
                    }
                  },
                  "properties": {
                    "id": { "type": "keyword" },
                    "numbers": { "type": "integer" },
                    "tags": { "type": "keyword" },
                    "message": { "type": "text" }
                  }
                }
                """;
        createIndex(indexName, mapping);
        try {
            index(indexName, ImmutableMap.of(
                    "id", "1",
                    "numbers", ImmutableList.of(1, 2),
                    "tags", ImmutableList.of("a", "b"),
                    "message", "fatal alpha"));
            index(indexName, ImmutableMap.of(
                    "id", "2",
                    "numbers", ImmutableList.of(1, 3),
                    "tags", ImmutableList.of("a"),
                    "message", "error beta"));
            index(indexName, ImmutableMap.of(
                    "id", "3",
                    "numbers", ImmutableList.of(2),
                    "tags", ImmutableList.of("b"),
                    "message", "other"));
            index(indexName, ImmutableMap.of(
                    "id", "4",
                    "numbers", ImmutableList.of(1, 2),
                    "tags", ImmutableList.of("c")));

            assertThat(query("SELECT id FROM " + indexName + " WHERE contains(tags, 'a') OR contains(tags, 'b')"))
                    .matches("VALUES VARCHAR '1', VARCHAR '2', VARCHAR '3'")
                    .isFullyPushedDown();

            // Two exact predicates on the same scalar field must coexist in the Remote Predicate IR. A legacy
            // one-entry-per-field map would overwrite one of these clauses.
            assertThat(query("SELECT id FROM " + indexName + " WHERE id LIKE '1%' AND id LIKE '%1'"))
                    .matches("VALUES VARCHAR '1'")
                    .isFullyPushedDown();

            // Document-scope conjunction allows different array elements to satisfy independent predicates.
            assertThat(query("SELECT id FROM " + indexName + " WHERE contains(numbers, 1) AND contains(numbers, 2)"))
                    .matches("VALUES VARCHAR '1', VARCHAR '4'")
                    .isFullyPushedDown();

            // P1.1 same-element semantics remain encapsulated inside any_match translation.
            assertThat(query("SELECT id FROM " + indexName + " WHERE any_match(numbers, x -> x > 1 AND x < 3)"))
                    .matches("VALUES VARCHAR '1', VARCHAR '3', VARCHAR '4'")
                    .isFullyPushedDown();

            assertThat(query("SELECT id FROM " + indexName + " WHERE contains(tags, 'a') OR cardinality(numbers) = 1"))
                    .matches("VALUES VARCHAR '1', VARCHAR '2', VARCHAR '3'")
                    .isNotFullyPushedDown(FilterNode.class);

            String catalogName = getSession().getCatalog().orElseThrow();
            Session safe = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "SAFE")
                    .build();
            Session unsafe = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "UNSAFE")
                    .build();

            // Analyzed-text LIKE has no general no-false-negative proof. SAFE keeps the complete OR in Trino instead of
            // relying on the residual to repair rows that a lossy remote candidate may already have removed.
            assertThat(query(safe, "SELECT id FROM " + indexName + " WHERE message LIKE 'fatal%' OR message LIKE 'error%'"))
                    .matches("VALUES VARCHAR '1', VARCHAR '2'")
                    .isNotFullyPushedDown(FilterNode.class);

            // UNSAFE explicitly accepts analyzer semantics. Both same-field predicates coexist under one document-scope
            // And instead of overwriting each other in a legacy one-predicate-per-field map.
            assertThat(query(unsafe, "SELECT id FROM " + indexName + " WHERE message LIKE '%fatal%' AND message LIKE '%alpha%'"))
                    .matches("VALUES VARCHAR '1'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(indexName);
        }
    }

    @Test
    public void testSafeAnalyzedTextDoesNotUseLossyCandidate()
            throws IOException
    {
        String indexName = "p1_safe_analyzed_lossless_" + randomNameSuffix();
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
        createIndexBody(indexName, body);
        try {
            index(indexName, ImmutableMap.of("name", "ngô văn", "id", "1"));
            index(indexName, ImmutableMap.of("name", "ngo van", "id", "2"));

            String catalogName = getSession().getCatalog().orElseThrow();
            Session safe = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "SAFE")
                    .build();

            // The analyzer indexes "ngô" as "ngo". A remote regexp containing "ngô" would produce a false negative
            // before Trino could run its residual. SAFE therefore keeps this predicate local and must still return id=1.
            assertThat(query(safe, "SELECT id FROM " + indexName + " WHERE name LIKE '%ngô%'"))
                    .matches("VALUES VARCHAR '1'")
                    .isNotFullyPushedDown(FilterNode.class);
        }
        finally {
            deleteIndex(indexName);
        }
    }

    private void createIndex(String indexName, @Language("JSON") String mapping)
            throws IOException
    {
        createIndexBody(indexName, "{\"mappings\": " + mapping + "}");
    }

    private void createIndexBody(String indexName, @Language("JSON") String body)
            throws IOException
    {
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(body);
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
