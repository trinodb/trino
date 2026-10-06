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
package io.trino.jsonpath;

import io.trino.Session;
import io.trino.json.Json;
import io.trino.json.JsonItems;
import io.trino.json.TypedValue;
import io.trino.jsonpath.ir.IrPathNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.PathNodes.contextVariable;
import static io.trino.sql.planner.PathNodes.memberAccessor;
import static io.trino.sql.planner.PathNodes.path;
import static io.trino.sql.planner.PathNodes.wildcardMemberAccessor;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJsonPathMemberAccessor
{
    @Test
    void testFastAndGeneralEvaluation()
    {
        Session session = testSessionBuilder().build();
        for (String document : List.of(
                "{}",
                "[]",
                "null",
                "1",
                "{\"a\":1}",
                "{\"a\":{\"b\":1},\"a\":{\"b\":2}}",
                "[ {\"a\":{\"b\":1}}, {}, null ]",
                "[{\"a\":{\"b\":1}},{\"a\":[{\"b\":2}]}]",
                "[[{\"a\":{\"b\":1}}]]",
                "{\"é\":{\"\":1},\"a\":{\"b\":null}}")) {
            for (Json input : List.of(Json.unchecked(utf8Slice(document)), JsonItems.fromText(utf8Slice(document)), JsonItems.parseToTree(utf8Slice(document)))) {
                for (boolean lax : List.of(false, true)) {
                    for (IrPathNode root : List.of(
                            contextVariable(),
                            memberAccessor(contextVariable(), "a"),
                            memberAccessor(memberAccessor(contextVariable(), "a"), "b"),
                            memberAccessor(memberAccessor(contextVariable(), "é"), ""),
                            wildcardMemberAccessor(contextVariable()))) {
                        JsonPathEvaluator evaluator = new JsonPathEvaluator(
                                path(lax, root),
                                session.toConnectorSession(),
                                PLANNER_CONTEXT.getMetadata(),
                                PLANNER_CONTEXT.getTypeManager(),
                                PLANNER_CONTEXT.getFunctionManager());
                        for (Object[] parameters : List.of(new Object[0], new Object[] {new TypedValue(BIGINT, 1L)})) {
                            PathEvaluationVisitor general = new PathEvaluationVisitor(
                                    lax,
                                    input,
                                    parameters,
                                    new JsonPathEvaluator.Invoker(session.toConnectorSession(), PLANNER_CONTEXT.getFunctionManager()),
                                    new CachingResolver(PLANNER_CONTEXT.getMetadata(), getCharVarcharCoercion(session)));
                            List<Json> expected;
                            try {
                                expected = general.process(root, new PathEvaluationContext());
                            }
                            catch (PathEvaluationException e) {
                                assertThatThrownBy(() -> evaluator.evaluate(input, parameters))
                                        .isInstanceOf(PathEvaluationException.class)
                                        .hasMessage(e.getMessage());
                                continue;
                            }
                            assertThat(evaluator.evaluate(input, parameters))
                                    .as("%s, lax=%s, parameters=%s, path=%s", document, lax, parameters.length, root)
                                    .containsExactlyElementsOf(expected);
                        }
                    }
                }
            }
        }
    }
}
