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
package io.trino.operator.scalar;

import io.trino.json.Json;
import io.trino.json.JsonItems;
import io.trino.json.TypedValue;
import io.trino.jsonpath.JsonPathInvocationContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.JsonQuery.ArrayWrapperBehavior;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.scalar.json.JsonQueryFunction.jsonQuery;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.sql.planner.PathNodes.contextVariable;
import static io.trino.sql.planner.PathNodes.path;
import static io.trino.sql.tree.JsonQuery.ArrayWrapperBehavior.WITHOUT;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.ERROR;
import static org.assertj.core.api.Assertions.assertThat;

class TestJsonQueryRepresentations
{
    private final PlannerContext plannerContext = new TestingFunctionResolution().getPlannerContext();

    @Test
    void testScalarRepresentations()
    {
        for (TypedValue scalar : List.of(
                new TypedValue(DATE, 20726L),
                new TypedValue(createTimeType(3), 123_000_000_000L),
                new TypedValue(createDecimalType(3, 2), 123L))) {
            Json encoded = Json.of(scalar.encoding());
            for (Json input : List.of(scalar, encoded)) {
                for (ArrayWrapperBehavior wrapper : ArrayWrapperBehavior.values()) {
                    Json result = jsonQuery(
                            plannerContext.getFunctionManager(),
                            plannerContext.getMetadata(),
                            plannerContext.getTypeManager(),
                            JSON_NO_PARAMETERS_ROW_TYPE,
                            new JsonPathInvocationContext(),
                            TEST_SESSION.toConnectorSession(),
                            input,
                            path(true, contextVariable()),
                            null,
                            wrapper.ordinal(),
                            ERROR.ordinal(),
                            ERROR.ordinal());
                    Json item = wrapper == WITHOUT ? result : result.arrayElement(0);
                    assertThat(item.materializeScalar().type()).isEqualTo(encoded.materializeScalar().type());
                    assertThat(item.encoding()).isEqualTo(scalar.encoding());
                    assertThat(JsonItems.toText(item)).isEqualTo(JsonItems.toText(scalar));
                }
            }
        }
    }

    @Test
    void testCharOutputRepresentations()
    {
        Json input = new TypedValue(createCharType(3), utf8Slice("a"));
        for (ArrayWrapperBehavior wrapper : ArrayWrapperBehavior.values()) {
            Json result = jsonQuery(
                    plannerContext.getFunctionManager(),
                    plannerContext.getMetadata(),
                    plannerContext.getTypeManager(),
                    JSON_NO_PARAMETERS_ROW_TYPE,
                    new JsonPathInvocationContext(),
                    TEST_SESSION.toConnectorSession(),
                    input,
                    path(true, contextVariable()),
                    null,
                    wrapper.ordinal(),
                    ERROR.ordinal(),
                    ERROR.ordinal());
            for (Json output : List.of(result, Json.of(result.encoding()))) {
                Json item = wrapper == WITHOUT ? output : output.arrayElement(0);
                assertThat(item.materializeScalar().type()).isEqualTo(VARCHAR);
                assertThat(item.materializeScalar().value()).isEqualTo(utf8Slice("a  "));
            }
        }
    }
}
