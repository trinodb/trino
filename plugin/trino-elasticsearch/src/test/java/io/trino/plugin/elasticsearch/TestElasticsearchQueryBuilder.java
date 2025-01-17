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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.decoders.DoubleDecoder;
import io.trino.plugin.elasticsearch.decoders.IntegerDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchQueryBuilder
{
    private static final ElasticsearchColumnHandle NAME = new ElasticsearchColumnHandle(ImmutableList.of("name"), VARCHAR, new IndexMetadata.PrimitiveType("text"), new VarcharDecoder.Descriptor("name"), true);
    private static final ElasticsearchColumnHandle AGE = new ElasticsearchColumnHandle(ImmutableList.of("age"), INTEGER, new IndexMetadata.PrimitiveType("int"), new IntegerDecoder.Descriptor("age"), true);
    private static final ElasticsearchColumnHandle SCORE = new ElasticsearchColumnHandle(ImmutableList.of("score"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("score"), true);
    private static final ElasticsearchColumnHandle LENGTH = new ElasticsearchColumnHandle(ImmutableList.of("length"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("length"), true);

    @Test
    public void testMatchAll()
    {
        assertQueryBuilder(
                ImmutableMap.of(),
                new MatchAllQueryBuilder());
    }

    @Test
    public void testOneConstraint()
    {
        // SingleValue
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.singleValue(INTEGER, 1L)),
                new BoolQueryBuilder().filter(new TermQueryBuilder(AGE.name(), 1L)));

        // Range
        assertQueryBuilder(
                ImmutableMap.of(SCORE, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 65.0, false, 80.0, true)), false)),
                new BoolQueryBuilder().filter(new RangeQueryBuilder(SCORE.name()).gt(65.0).lte(80.0)));

        // List
        assertQueryBuilder(
                ImmutableMap.of(NAME, Domain.multipleValues(VARCHAR, ImmutableList.of("alice", "bob"))),
                new BoolQueryBuilder().filter(
                        new BoolQueryBuilder()
                                .should(new TermQueryBuilder(NAME.name(), "alice"))
                                .should(new TermQueryBuilder(NAME.name(), "bob"))));
        // all
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.all(INTEGER)),
                new MatchAllQueryBuilder());

        // notNull
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.notNull(INTEGER)),
                new BoolQueryBuilder().filter(new ExistsQueryBuilder(AGE.name())));

        // isNull
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.onlyNull(INTEGER)),
                new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(AGE.name())));

        // isNullAllowed
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.singleValue(INTEGER, 1L, true)),
                new BoolQueryBuilder().filter(
                        new BoolQueryBuilder()
                                .should(new TermQueryBuilder(AGE.name(), 1L))
                                .should(new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(AGE.name())))));
    }

    @Test
    public void testMultiConstraint()
    {
        assertQueryBuilder(
                ImmutableMap.of(
                        AGE, Domain.singleValue(INTEGER, 1L),
                        SCORE, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 65.0, false, 80.0, true)), false)),
                new BoolQueryBuilder()
                        .filter(new TermQueryBuilder(AGE.name(), 1L))
                        .filter(new RangeQueryBuilder(SCORE.name()).gt(65.0).lte(80.0)));

        assertQueryBuilder(
                ImmutableMap.of(
                        LENGTH, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 160.0, true, 180.0, true)), false),
                        SCORE, Domain.create(ValueSet.ofRanges(
                                Range.range(DOUBLE, 65.0, false, 80.0, true),
                                Range.equal(DOUBLE, 90.0)), false)),
                new BoolQueryBuilder()
                        .filter(new RangeQueryBuilder(LENGTH.name()).gte(160.0).lte(180.0))
                        .filter(new BoolQueryBuilder()
                                .should(new RangeQueryBuilder(SCORE.name()).gt(65.0).lte(80.0))
                                .should(new TermQueryBuilder(SCORE.name(), 90.0))));

        assertQueryBuilder(
                ImmutableMap.of(
                        AGE, Domain.singleValue(INTEGER, 10L),
                        SCORE, Domain.onlyNull(DOUBLE)),
                new BoolQueryBuilder()
                        .filter(new TermQueryBuilder(AGE.name(), 10L))
                        .mustNot(new ExistsQueryBuilder(SCORE.name())));
    }

    private static void assertQueryBuilder(Map<ElasticsearchColumnHandle, Domain> domains, QueryBuilder expected)
    {
        QueryBuilder actual = buildSearchQuery(TupleDomain.withColumnDomains(domains), Optional.empty(), Map.of());
        assertThat(actual).isEqualTo(expected);
    }
}
