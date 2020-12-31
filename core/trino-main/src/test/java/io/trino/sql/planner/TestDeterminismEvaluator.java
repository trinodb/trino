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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeterminismEvaluator
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testSanity()
    {
        assertFalse(DeterminismEvaluator.isDeterministic(function("rand"), metadata));
        assertFalse(DeterminismEvaluator.isDeterministic(function("random"), metadata));
        assertFalse(DeterminismEvaluator.isDeterministic(function("shuffle", ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(new NullLiteral())), metadata));
        assertFalse(DeterminismEvaluator.isDeterministic(function("uuid"), metadata));
        assertTrue(DeterminismEvaluator.isDeterministic(function("abs", ImmutableList.of(DOUBLE), ImmutableList.of(input("symbol"))), metadata));
        assertFalse(DeterminismEvaluator.isDeterministic(function("abs", ImmutableList.of(DOUBLE), ImmutableList.of(function("rand"))), metadata));
        assertTrue(DeterminismEvaluator.isDeterministic(
                function(
                        "abs",
                        ImmutableList.of(DOUBLE),
                        ImmutableList.of(function("abs", ImmutableList.of(DOUBLE), ImmutableList.of(input("symbol"))))),
                metadata));
    }

    private FunctionCall function(String name)
    {
        return function(name, ImmutableList.of(), ImmutableList.of());
    }

    private FunctionCall function(String name, List<Type> types, List<Expression> arguments)
    {
        return new FunctionCallBuilder(metadata)
                .setName(QualifiedName.of(name))
                .setArguments(types, arguments)
                .build();
    }

    private static Identifier input(String symbol)
    {
        return new Identifier(symbol);
    }
}
