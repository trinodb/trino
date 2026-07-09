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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.SimplifyCharLength;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyCharLength
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);
    private static final ResolvedFunction CHAR_LENGTH = FUNCTIONS.resolveFunction("length", fromTypes(createCharType(5)));
    private static final ResolvedFunction VARCHAR_LENGTH = FUNCTIONS.resolveFunction("length", fromTypes(VARCHAR));

    @Test
    void testNullableCharIsRewrittenToNullPreservingConstant()
    {
        Reference value = new Reference(createCharType(5), "x");
        assertThat(optimize(new Call(CHAR_LENGTH, ImmutableList.of(value))))
                .describedAs("a nullable char(n) preserves null on null input")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(new WhenClause(new IsNull(value), new Constant(BIGINT, null))),
                        new Constant(BIGINT, 5L))));
    }

    @Test
    void testNonNullCharIsFoldedToConstant()
    {
        assertThat(optimize(new Call(CHAR_LENGTH, ImmutableList.of(new Constant(createCharType(5), utf8Slice("hello"))))))
                .describedAs("a non-null char(n) folds directly to the declared length")
                .isEqualTo(Optional.of(new Constant(BIGINT, 5L)));
    }

    @Test
    void testLengthOfVarcharIsNotRewritten()
    {
        assertThat(optimize(new Call(VARCHAR_LENGTH, ImmutableList.of(new Reference(VARCHAR, "x")))))
                .isEmpty();
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyCharLength(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
