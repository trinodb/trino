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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import io.trino.plugin.prometheus.expression.LabelFilterExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPrometheusTableHandle
{
    private final PrometheusTableHandle tableHandle = newTableHandle("schemaName", "tableName");

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<PrometheusTableHandle> codec = jsonCodec(PrometheusTableHandle.class);
        String json = codec.toJson(tableHandle);
        PrometheusTableHandle copy = codec.fromJson(json);
        assertThat(copy).isEqualTo(tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(newTableHandle("schema", "table"), newTableHandle("schema", "table"))
                .addEquivalentGroup(newTableHandle("schemaX", "table"), newTableHandle("schemaX", "table"))
                .addEquivalentGroup(newTableHandle("schema", "tableX"), newTableHandle("schema", "tableX"))
                .check();
    }

    public static PrometheusTableHandle newTableHandle(String schemaName, String tableName)
    {
        return newTableHandle(schemaName, tableName, TupleDomain.all(), ImmutableList.of());
    }

    public static PrometheusTableHandle newTableHandle(String schemaName, String tableName, TupleDomain<ColumnHandle> predicate, List<LabelFilterExpression> expressions)
    {
        return new PrometheusTableHandle(schemaName, tableName, predicate, expressions);
    }
}
