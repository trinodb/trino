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
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeterminismEvaluator
{
    @Test
    public void testDeterminismEvaluator()
    {
        Metadata metadata = createTestMetadataManager();
        DeterminismEvaluator determinismEvaluator = new DeterminismEvaluator(metadata);

        CallExpression random = new CallExpression(
                metadata.resolveFunction(QualifiedName.of("random"), fromTypes(BIGINT)),
                singletonList(constant(10L, BIGINT)));
        assertFalse(determinismEvaluator.isDeterministic(random));

        InputReferenceExpression col0 = field(0, BIGINT);
        ResolvedFunction lessThan = metadata.resolveOperator(LESS_THAN, ImmutableList.of(BIGINT, BIGINT));

        CallExpression lessThanExpression = new CallExpression(lessThan, ImmutableList.of(col0, constant(10L, BIGINT)));
        assertTrue(determinismEvaluator.isDeterministic(lessThanExpression));

        CallExpression lessThanRandomExpression = new CallExpression(lessThan, ImmutableList.of(col0, random));
        assertFalse(determinismEvaluator.isDeterministic(lessThanRandomExpression));
    }
}
