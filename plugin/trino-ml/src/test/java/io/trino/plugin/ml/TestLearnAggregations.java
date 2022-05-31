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
package io.trino.plugin.ml;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.RowPageBuilder;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.Aggregator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.plugin.ml.type.ClassifierParametricType;
import io.trino.plugin.ml.type.ModelType;
import io.trino.plugin.ml.type.RegressorType;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.QualifiedName;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.Test;

import java.util.OptionalInt;
import java.util.Random;

import static io.trino.metadata.InternalFunctionBundle.extractFunctions;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeSignatureParameter.typeParameter;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.StructuralTestUtil.mapBlockOf;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestLearnAggregations
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION;

    static {
        TransactionManager transactionManager = createTestTransactionManager();
        PlannerContext plannerContext = plannerContextBuilder()
                .withTransactionManager(transactionManager)
                .addParametricType(new ClassifierParametricType())
                .addType(ModelType.MODEL)
                .addType(RegressorType.REGRESSOR)
                .addFunctions(extractFunctions(new MLPlugin().getFunctions()))
                .build();
        FUNCTION_RESOLUTION = new TestingFunctionResolution(transactionManager, plannerContext);
    }

    @Test
    public void testLearn()
    {
        TestingAggregationFunction aggregationFunction = FUNCTION_RESOLUTION.getAggregateFunction(
                QualifiedName.of("learn_classifier"),
                fromTypeSignatures(BIGINT.getTypeSignature(), mapType(BIGINT.getTypeSignature(), DOUBLE.getTypeSignature())));
        assertLearnClassifier(aggregationFunction.createAggregatorFactory(SINGLE, ImmutableList.of(0, 1), OptionalInt.empty()).createAggregator());
    }

    @Test
    public void testLearnLibSvm()
    {
        TestingAggregationFunction aggregationFunction = FUNCTION_RESOLUTION.getAggregateFunction(
                QualifiedName.of("learn_libsvm_classifier"),
                fromTypeSignatures(BIGINT.getTypeSignature(), mapType(BIGINT.getTypeSignature(), DOUBLE.getTypeSignature()), VARCHAR.getTypeSignature()));
        assertLearnClassifier(aggregationFunction.createAggregatorFactory(SINGLE, ImmutableList.of(0, 1, 2), OptionalInt.empty()).createAggregator());
    }

    private static void assertLearnClassifier(Aggregator aggregator)
    {
        aggregator.processPage(getPage());
        BlockBuilder finalOut = aggregator.getType().createBlockBuilder(null, 1);
        aggregator.evaluate(finalOut);
        Block block = finalOut.build();
        Slice slice = aggregator.getType().getSlice(block, 0);
        Model deserialized = ModelUtils.deserialize(slice);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof Classifier, "deserialized model is not a classifier");
    }

    private static Page getPage()
    {
        Type mapType = TESTING_TYPE_MANAGER.getParameterizedType("map", ImmutableList.of(typeParameter(BIGINT.getTypeSignature()), typeParameter(DOUBLE.getTypeSignature())));
        int datapoints = 100;
        RowPageBuilder builder = RowPageBuilder.rowPageBuilder(BIGINT, mapType, VARCHAR);
        Random rand = new Random(0);
        for (int i = 0; i < datapoints; i++) {
            long label = rand.nextDouble() < 0.5 ? 0 : 1;
            builder.row(label, mapBlockOf(BIGINT, DOUBLE, 0L, label + rand.nextGaussian()), "C=1");
        }

        return builder.build();
    }
}
