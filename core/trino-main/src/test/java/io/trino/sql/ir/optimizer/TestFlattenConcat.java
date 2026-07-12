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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.ArrayType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.FlattenConcat;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFlattenConcat
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ArrayType BIGINT_ARRAY = new ArrayType(BIGINT);

    private static final ResolvedFunction CONCAT_2 = FUNCTIONS.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR));
    private static final ResolvedFunction CONCAT_3 = FUNCTIONS.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR, VARCHAR));
    private static final ResolvedFunction CONCAT_4 = FUNCTIONS.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR, VARCHAR, VARCHAR));
    private static final ResolvedFunction VARBINARY_CONCAT = FUNCTIONS.resolveFunction("concat", fromTypes(VARBINARY, VARBINARY));
    private static final ResolvedFunction VARBINARY_CONCAT_3 = FUNCTIONS.resolveFunction("concat", fromTypes(VARBINARY, VARBINARY, VARBINARY));
    private static final ResolvedFunction ARRAY_CONCAT = FUNCTIONS.resolveFunction("concat", fromTypes(BIGINT_ARRAY, BIGINT_ARRAY));
    private static final ResolvedFunction ARRAY_CONCAT_3 = FUNCTIONS.resolveFunction("concat", fromTypes(BIGINT_ARRAY, BIGINT_ARRAY, BIGINT_ARRAY));
    private static final ResolvedFunction ELEMENT_ARRAY_CONCAT = FUNCTIONS.resolveFunction("concat", fromTypes(BIGINT, BIGINT_ARRAY));

    @Test
    void testFlatten()
    {
        Reference a = new Reference(VARCHAR, "a");
        Reference b = new Reference(VARCHAR, "b");
        Reference c = new Reference(VARCHAR, "c");
        Reference d = new Reference(VARCHAR, "d");

        assertThat(optimize(
                new Call(CONCAT_2, ImmutableList.of(new Call(CONCAT_2, ImmutableList.of(a, b)), c))))
                .describedAs("left-nested")
                .isEqualTo(Optional.of(new Call(CONCAT_3, ImmutableList.of(a, b, c))));

        assertThat(optimize(
                new Call(CONCAT_2, ImmutableList.of(a, new Call(CONCAT_2, ImmutableList.of(b, c))))))
                .describedAs("right-nested")
                .isEqualTo(Optional.of(new Call(CONCAT_3, ImmutableList.of(a, b, c))));

        assertThat(optimize(
                new Call(CONCAT_2, ImmutableList.of(
                        new Call(CONCAT_2, ImmutableList.of(a, b)),
                        new Call(CONCAT_2, ImmutableList.of(c, d))))))
                .describedAs("nested on both sides")
                .isEqualTo(Optional.of(new Call(CONCAT_4, ImmutableList.of(a, b, c, d))));
    }

    @Test
    void testVarbinary()
    {
        Reference a = new Reference(VARBINARY, "a");
        Reference b = new Reference(VARBINARY, "b");
        Reference c = new Reference(VARBINARY, "c");

        assertThat(optimize(
                new Call(VARBINARY_CONCAT, ImmutableList.of(new Call(VARBINARY_CONCAT, ImmutableList.of(a, b)), c))))
                .isEqualTo(Optional.of(new Call(VARBINARY_CONCAT_3, ImmutableList.of(a, b, c))));
    }

    @Test
    void testArray()
    {
        Reference a = new Reference(BIGINT_ARRAY, "a");
        Reference b = new Reference(BIGINT_ARRAY, "b");
        Reference c = new Reference(BIGINT_ARRAY, "c");

        assertThat(optimize(
                new Call(ARRAY_CONCAT, ImmutableList.of(new Call(ARRAY_CONCAT, ImmutableList.of(a, b)), c))))
                .isEqualTo(Optional.of(new Call(ARRAY_CONCAT_3, ImmutableList.of(a, b, c))));
    }

    @Test
    void testDoesNotFire()
    {
        assertThat(optimize(
                new Call(CONCAT_2, ImmutableList.of(new Reference(VARCHAR, "a"), new Reference(VARCHAR, "b")))))
                .describedAs("no nested concat")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Call(ARRAY_CONCAT, ImmutableList.of(
                        new Call(ELEMENT_ARRAY_CONCAT, ImmutableList.of(new Reference(BIGINT, "e"), new Reference(BIGINT_ARRAY, "a"))),
                        new Reference(BIGINT_ARRAY, "b")))))
                .describedAs("element || array child has a non-uniform signature")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Call(ELEMENT_ARRAY_CONCAT, ImmutableList.of(
                        new Reference(BIGINT, "e"),
                        new Call(ARRAY_CONCAT, ImmutableList.of(new Reference(BIGINT_ARRAY, "a"), new Reference(BIGINT_ARRAY, "b")))))))
                .describedAs("element || array parent has a non-uniform signature")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testFlattensFullyWithOptimizer()
    {
        Reference a = new Reference(VARCHAR, "a");
        Reference b = new Reference(VARCHAR, "b");
        Reference c = new Reference(VARCHAR, "c");
        Reference d = new Reference(VARCHAR, "d");

        // a || b || c || d parses as concat(concat(concat(a, b), c), d)
        assertThat(newOptimizer(PLANNER_CONTEXT).process(
                new Call(CONCAT_2, ImmutableList.of(
                        new Call(CONCAT_2, ImmutableList.of(new Call(CONCAT_2, ImmutableList.of(a, b)), c)),
                        d)),
                testSession(),
                new SymbolAllocator(),
                ImmutableMap.of()))
                .isEqualTo(Optional.of(new Call(CONCAT_4, ImmutableList.of(a, b, c, d))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new FlattenConcat(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
