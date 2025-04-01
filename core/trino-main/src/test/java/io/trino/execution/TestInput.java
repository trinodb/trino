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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestInput
{
    private static final JsonCodec<Input> codec = JsonCodec.jsonCodec(Input.class);

    @Test
    public void testRoundTrip()
    {
        Input expected = new Input(
                Optional.of("connectorName"),
                "connectorId",
                new CatalogVersion("default"),
                "schema",
                "table",
                Optional.empty(),
                ImmutableList.of(
                        new Column("column1", "string"),
                        new Column("column2", "string"),
                        new Column("column3", "string")),
                new PlanFragmentId("fragment"),
                new PlanNodeId("plan-node"));

        String json = codec.toJson(expected);
        Input actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
    }
}
