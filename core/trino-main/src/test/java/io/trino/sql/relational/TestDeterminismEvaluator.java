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
package io.trino.sql.relational;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeterminismEvaluator
{
    @Test
    public void testDeterminismEvaluator()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution();

        CallExpression random = new CallExpression(
                functionResolution.resolveFunction(QualifiedName.of("random"), fromTypes(BIGINT)),
                singletonList(constant(10L, BIGINT)));
        assertThat(isDeterministic(random)).isFalse();

        InputReferenceExpression col0 = field(0, BIGINT);
        ResolvedFunction lessThan = functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(BIGINT, BIGINT));

        CallExpression lessThanExpression = new CallExpression(lessThan, ImmutableList.of(col0, constant(10L, BIGINT)));
        assertThat(isDeterministic(lessThanExpression)).isTrue();

        CallExpression lessThanRandomExpression = new CallExpression(lessThan, ImmutableList.of(col0, random));
        assertThat(isDeterministic(lessThanRandomExpression)).isFalse();
    }
}
