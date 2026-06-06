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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchQueryBuilder
{
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    private static final ElasticsearchColumnHandle NAME = new ElasticsearchColumnHandle(ImmutableList.of("name"), VARCHAR, new IndexMetadata.PrimitiveType("text"), new VarcharDecoder.Descriptor("name"), true);
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

        // List
        assertQueryBuilder(
                ImmutableMap.of(NAME, Domain.multipleValues(VARCHAR, ImmutableList.of("alice", "bob"))),
                """
                {"bool":{"filter":[{"bool":{"should":[{"term":{"name":"alice"}},{"term":{"name":"bob"}}]}}]}}""");

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

    private static void assertQueryBuilder(Map<ElasticsearchColumnHandle, Domain> domains, String expected)
            throws IOException
    {
        JsonNode actual = buildSearchQuery(TupleDomain.withColumnDomains(domains), Optional.empty(), Map.of());
        // Compare as normalized JSON trees to handle numeric type differences (LongNode vs IntNode)
        assertThat(JSON_MAPPER.readTree(actual.toString())).isEqualTo(JSON_MAPPER.readTree(expected));
    }
}
