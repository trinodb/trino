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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonMapperProvider;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.decoders.DoubleDecoder;
import io.trino.plugin.elasticsearch.decoders.IntegerDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchQueryBuilder
{
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    private static final ElasticsearchColumnHandle NAME = new ElasticsearchColumnHandle(ImmutableList.of("name"), VARCHAR, new IndexMetadata.PrimitiveType("text"), new VarcharDecoder.Descriptor("name"), true);
    private static final ElasticsearchColumnHandle ANALYZED_TEXT = new ElasticsearchColumnHandle(ImmutableList.of("text_column"), VARCHAR, new IndexMetadata.PrimitiveType("text"), new VarcharDecoder.Descriptor("text_column"), false);
    private static final ElasticsearchColumnHandle AGE = new ElasticsearchColumnHandle(ImmutableList.of("age"), INTEGER, new IndexMetadata.PrimitiveType("int"), new IntegerDecoder.Descriptor("age"), true);
    private static final ElasticsearchColumnHandle SCORE = new ElasticsearchColumnHandle(ImmutableList.of("score"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("score"), true);
    private static final ElasticsearchColumnHandle LENGTH = new ElasticsearchColumnHandle(ImmutableList.of("length"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("length"), true);

    @Test
    public void testMatchAll()
            throws IOException
    {
        assertQueryBuilder(
                ImmutableMap.of(),
                """
                {"match_all":{}}""");
    }

    @Test
    public void testOneConstraint()
            throws IOException
    {
        // SingleValue
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.singleValue(INTEGER, 1L)),
                """
                {"bool":{"filter":[{"term":{"age":1}}]}}""");

        // Range
        assertQueryBuilder(
                ImmutableMap.of(SCORE, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 65.0, false, 80.0, true)), false)),
                """
                {"bool":{"filter":[{"range":{"score":{"gt":65.0,"lte":80.0}}}]}}""");

        // Multiple discrete values are emitted as one native terms query, not a bool.should of term clauses.
        assertQueryBuilder(
                ImmutableMap.of(NAME, Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("alice"), utf8Slice("bob")))),
                """
                {"bool":{"filter":[{"terms":{"name":["alice","bob"]}}]}}""");

        // all
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.all(INTEGER)),
                """
                {"match_all":{}}""");

        // notNull
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.notNull(INTEGER)),
                """
                {"bool":{"filter":[{"exists":{"field":"age"}}]}}""");

        // isNull
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.onlyNull(INTEGER)),
                """
                {"bool":{"must_not":[{"exists":{"field":"age"}}]}}""");

        // isNullAllowed
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.singleValue(INTEGER, 1L, true)),
                """
                {"bool":{"filter":[{"bool":{"should":[{"term":{"age":1}},{"bool":{"must_not":[{"exists":{"field":"age"}}]}}]}}]}}""");
    }

    @Test
    public void testLargeDiscreteConstraintUsesSingleTermsQuery()
            throws IOException
    {
        List<Long> values = IntStream.range(0, 2_000)
                .mapToObj(value -> (long) value)
                .toList();
        JsonNode query = buildSearchQuery(
                TupleDomain.withColumnDomains(Map.of(AGE, Domain.multipleValues(INTEGER, values))),
                Optional.empty(),
                Map.of(),
                Map.of(),
                Map.of());

        JsonNode filter = query.path("bool").path("filter").get(0);
        assertThat(filter.has("terms")).isTrue();
        assertThat(filter.path("terms").path("age")).hasSize(2_000);
        assertThat(query.toString()).doesNotContain("\"should\"");
    }

    @Test
    public void testLegacyAndRemotePredicateComposition()
            throws IOException
    {
        ElasticsearchRemotePredicate remotePredicate = new ElasticsearchRemotePredicate.And(List.of(
                new ElasticsearchRemotePredicate.Term("status.keyword", "active"),
                new ElasticsearchRemotePredicate.Term("age", 42L)));

        JsonNode query = buildSearchQuery(
                TupleDomain.withColumnDomains(Map.of(AGE, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 18L)), false))),
                Optional.empty(),
                Map.of(),
                Map.of(),
                Map.of(),
                Optional.of(remotePredicate));

        JsonNode filter = query.path("bool").path("filter");
        assertThat(filter).hasSize(2);
        assertThat(filter.get(0).path("range").path("age").path("gt").asLong()).isEqualTo(18L);
        assertThat(filter.get(1).path("bool").path("filter")).hasSize(2);
        assertThat(query.toString()).contains("status.keyword", "active", "\"age\":42");
    }

    @Test
    public void testMultiConstraint()
            throws IOException
    {
        assertQueryBuilder(
                ImmutableMap.of(
                        AGE, Domain.singleValue(INTEGER, 1L),
                        SCORE, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 65.0, false, 80.0, true)), false)),
                """
                {"bool":{"filter":[{"term":{"age":1}},{"range":{"score":{"gt":65.0,"lte":80.0}}}]}}""");

        assertQueryBuilder(
                ImmutableMap.of(
                        LENGTH, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 160.0, true, 180.0, true)), false),
                        SCORE, Domain.create(ValueSet.ofRanges(
                                Range.range(DOUBLE, 65.0, false, 80.0, true),
                                Range.equal(DOUBLE, 90.0)), false)),
                """
                {"bool":{"filter":[{"range":{"length":{"gte":160.0,"lte":180.0}}},{"bool":{"should":[{"range":{"score":{"gt":65.0,"lte":80.0}}},{"term":{"score":90.0}}]}}]}}""");

        assertQueryBuilder(
                ImmutableMap.of(
                        AGE, Domain.singleValue(INTEGER, 10L),
                        SCORE, Domain.onlyNull(DOUBLE)),
                """
                {"bool":{"filter":[{"term":{"age":10}}],"must_not":[{"exists":{"field":"score"}}]}}""");
    }

    @Test
    public void testMatchPhrase()
            throws IOException
    {
        // A single-value domain on analyzed text is the lowering bridge for an UNSAFE full-text match_phrase.
        JsonNode actual = buildSearchQuery(
                TupleDomain.withColumnDomains(Map.of(ANALYZED_TEXT, Domain.singleValue(VARCHAR, utf8Slice("ngô văn")))),
                Optional.empty(),
                Map.of(),
                Map.of(),
                Map.of());
        assertThat(JSON_MAPPER.readTree(actual.toString()))
                .isEqualTo(JSON_MAPPER.readTree(
                        """
                        {"bool":{"filter":[{"match_phrase":{"text_column":"ngô văn"}}]}}"""));
    }

    @Test
    public void testMatchPhrasePrefix()
            throws IOException
    {
        // A full-text LIKE prefix on analyzed text is translated to a match_phrase_prefix query
        JsonNode actual = buildSearchQuery(TupleDomain.all(), Optional.empty(), Map.of(), Map.of(), Map.of("text_column", "soome te"));
        assertThat(JSON_MAPPER.readTree(actual.toString()))
                .isEqualTo(JSON_MAPPER.readTree(
                        """
                        {"bool":{"filter":[{"match_phrase_prefix":{"text_column":"soome te"}}]}}"""));
    }

    private static void assertQueryBuilder(Map<ElasticsearchColumnHandle, Domain> domains, String expected)
            throws IOException
    {
        JsonNode actual = buildSearchQuery(TupleDomain.withColumnDomains(domains), Optional.empty(), Map.of(), Map.of(), Map.of());
        // Compare as normalized JSON trees to handle numeric type differences (LongNode vs IntNode)
        assertThat(JSON_MAPPER.readTree(actual.toString())).isEqualTo(JSON_MAPPER.readTree(expected));
    }
}
