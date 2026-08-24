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

import io.trino.plugin.elasticsearch.client.IndexMetadata.DateTimeType;
import io.trino.plugin.elasticsearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.elasticsearch.decoders.BooleanDecoder;
import io.trino.plugin.elasticsearch.decoders.IntegerDecoder;
import io.trino.plugin.elasticsearch.decoders.TimestampDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchArrayPredicateTranslator
{
    @Test
    public void testContainsPrimitiveArray()
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                List.of("Numbers"),
                arrayType,
                new PrimitiveType("integer"),
                new IntegerDecoder.Descriptor("Numbers"),
                false);

        Call contains = new Call(
                BOOLEAN,
                new FunctionName("contains"),
                List.of(new Variable("numbers", arrayType), new Constant(42L, INTEGER)));

        assertThat(ElasticsearchArrayPredicateTranslator.translate(contains, Map.of("numbers", column)))
                .contains(new ElasticsearchRemotePredicate.Term("Numbers", 42L));
    }

    @Test
    public void testContainsBooleanArray()
    {
        ArrayType arrayType = new ArrayType(BOOLEAN);
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                List.of("Flags"),
                arrayType,
                new PrimitiveType("boolean"),
                new BooleanDecoder.Descriptor("Flags"),
                false);

        Call contains = new Call(
                BOOLEAN,
                new FunctionName("contains"),
                List.of(new Variable("flags", arrayType), new Constant(true, BOOLEAN)));

        assertThat(ElasticsearchArrayPredicateTranslator.translate(contains, Map.of("flags", column)))
                .contains(new ElasticsearchRemotePredicate.Term("Flags", true));
    }

    @Test
    public void testContainsTimestampArray()
    {
        ArrayType arrayType = new ArrayType(TIMESTAMP_MILLIS);
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                List.of("Times"),
                arrayType,
                new DateTimeType(List.of("strict_date_optional_time", "epoch_millis")),
                new TimestampDecoder.Descriptor("Times"),
                false);

        Call contains = new Call(
                BOOLEAN,
                new FunctionName("contains"),
                List.of(new Variable("times", arrayType), new Constant(1_000_000L, TIMESTAMP_MILLIS)));

        assertThat(ElasticsearchArrayPredicateTranslator.translate(contains, Map.of("times", column)))
                .contains(new ElasticsearchRemotePredicate.Term("Times", "1970-01-01T00:00:01"));
    }

    @Test
    public void testContainsTextArrayUsesKeywordSubfield()
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                List.of("Tags"),
                arrayType,
                new PrimitiveType("text", Optional.of("keyword")),
                new VarcharDecoder.Descriptor("Tags"),
                false);

        Call contains = new Call(
                BOOLEAN,
                new FunctionName("contains"),
                List.of(new Variable("tags", arrayType), new Constant(utf8Slice("ExactValue"), VARCHAR)));

        assertThat(ElasticsearchArrayPredicateTranslator.translate(contains, Map.of("tags", column)))
                .contains(new ElasticsearchRemotePredicate.Term("Tags.keyword", "ExactValue"));
    }

    @Test
    public void testAnalyzedTextArrayRemainsResidual()
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                List.of("Tags"),
                arrayType,
                new PrimitiveType("text"),
                new VarcharDecoder.Descriptor("Tags"),
                false);
        Call contains = new Call(
                BOOLEAN,
                new FunctionName("contains"),
                List.of(new Variable("tags", arrayType), new Constant(utf8Slice("value"), VARCHAR)));

        assertThat(ElasticsearchArrayPredicateTranslator.translate(contains, Map.of("tags", column))).isEmpty();
    }

    @Test
    public void testArraysOverlapUsesTerms()
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                List.of("Numbers"),
                arrayType,
                new PrimitiveType("integer"),
                new IntegerDecoder.Descriptor("Numbers"),
                false);
        Call arraysOverlap = new Call(
                BOOLEAN,
                new FunctionName("arrays_overlap"),
                List.of(new Variable("numbers", arrayType), new Constant(integerBlock(1, 2, 3), arrayType)));

        assertThat(ElasticsearchArrayPredicateTranslator.translate(arraysOverlap, Map.of("numbers", column)))
                .contains(new ElasticsearchRemotePredicate.Terms("Numbers", List.of(1L, 2L, 3L)));
    }

    @Test
    public void testArraysOverlapWithEmptyConstantArrayRemainsResidual()
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                List.of("Numbers"),
                arrayType,
                new PrimitiveType("integer"),
                new IntegerDecoder.Descriptor("Numbers"),
                false);
        Call arraysOverlap = new Call(
                BOOLEAN,
                new FunctionName("arrays_overlap"),
                List.of(new Variable("numbers", arrayType), new Constant(integerBlock(), arrayType)));

        assertThat(ElasticsearchArrayPredicateTranslator.translate(arraysOverlap, Map.of("numbers", column))).isEmpty();
    }

    @Test
    public void testArraysOverlapWithNullElementRemainsResidual()
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                List.of("Numbers"),
                arrayType,
                new PrimitiveType("integer"),
                new IntegerDecoder.Descriptor("Numbers"),
                false);
        BlockBuilder builder = INTEGER.createBlockBuilder(null, 2);
        INTEGER.writeLong(builder, 1);
        builder.appendNull();
        Call arraysOverlap = new Call(
                BOOLEAN,
                new FunctionName("arrays_overlap"),
                List.of(new Variable("numbers", arrayType), new Constant(builder.build(), arrayType)));

        assertThat(ElasticsearchArrayPredicateTranslator.translate(arraysOverlap, Map.of("numbers", column))).isEmpty();
    }

    private static Block integerBlock(long... values)
    {
        BlockBuilder builder = INTEGER.createBlockBuilder(null, values.length);
        for (long value : values) {
            INTEGER.writeLong(builder, value);
        }
        return builder.build();
    }
}
