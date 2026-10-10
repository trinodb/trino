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

import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregateQueryPageSource
{
    @Test
    public void testExtractSingleValueReturnsNullForInfiniteValue()
    {
        assertThat(AggregateQueryPageSource.extractSingleValue(DoubleNode.valueOf(Double.POSITIVE_INFINITY))).isNull();
        assertThat(AggregateQueryPageSource.extractSingleValue(DoubleNode.valueOf(Double.NEGATIVE_INFINITY))).isNull();
        assertThat(AggregateQueryPageSource.extractSingleValue(TextNode.valueOf("Infinity"))).isNull();
        assertThat(AggregateQueryPageSource.extractSingleValue(TextNode.valueOf("-Infinity"))).isNull();
    }

    @Test
    public void testExtractSingleValueReturnsNullForNullValue()
    {
        assertThat(AggregateQueryPageSource.extractSingleValue(NullNode.instance)).isNull();
    }

    @Test
    public void testExtractSingleValueReturnsFiniteValue()
    {
        assertThat(AggregateQueryPageSource.extractSingleValue(DoubleNode.valueOf(42.5))).isEqualTo(42.5);
        assertThat(AggregateQueryPageSource.extractSingleValue(TextNode.valueOf("42.5"))).isEqualTo(42.5);
    }

    @Test
    public void testExtractSingleValueReturnsNaN()
    {
        assertThat(AggregateQueryPageSource.extractSingleValue(DoubleNode.valueOf(Double.NaN))).isNaN();
        assertThat(AggregateQueryPageSource.extractSingleValue(TextNode.valueOf("NaN"))).isNaN();
    }

    @Test
    public void testExtractSumFromStatsValueReturnsNullWhenStatsHasNoValues()
    {
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(JsonNodeFactory.instance.objectNode()
                .putNull("min")
                .put("sum", 0.0))).isNull();
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(JsonNodeFactory.instance.objectNode()
                .put("min", Double.POSITIVE_INFINITY)
                .put("sum", 0.0))).isNull();
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(JsonNodeFactory.instance.objectNode()
                .put("min", "Infinity")
                .put("sum", 0.0))).isNull();
    }

    @Test
    public void testExtractSumFromStatsValueReturnsSumForNonEmptyStats()
    {
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(JsonNodeFactory.instance.objectNode()
                .put("min", 10.0)
                .put("sum", 30.0))).isEqualTo(30.0);
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(JsonNodeFactory.instance.objectNode()
                .put("min", 10.0)
                .put("sum", Double.POSITIVE_INFINITY))).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(JsonNodeFactory.instance.objectNode()
                .put("min", 10.0)
                .put("sum", "Infinity"))).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(JsonNodeFactory.instance.objectNode()
                .put("min", 10.0)
                .put("sum", Double.NaN))).isNaN();
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(JsonNodeFactory.instance.objectNode()
                .put("min", 10.0)
                .put("sum", "NaN"))).isNaN();
    }
}
