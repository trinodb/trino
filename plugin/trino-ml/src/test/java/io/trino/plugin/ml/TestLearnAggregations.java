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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.FeaturesConfig;
import io.trino.RowPageBuilder;
import io.trino.client.NodeVersion;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.metadata.TypeRegistry;
import io.trino.operator.aggregation.Aggregator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.plugin.ml.type.ClassifierParametricType;
import io.trino.plugin.ml.type.ModelType;
import io.trino.plugin.ml.type.RegressorType;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.tree.QualifiedName;
import io.trino.transaction.TransactionManager;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import org.testng.annotations.Test;

import java.util.OptionalInt;
import java.util.Random;

import static io.trino.metadata.FunctionExtractor.extractFunctions;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeSignatureParameter.typeParameter;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
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
        TypeOperators typeOperators = new TypeOperators();
        TypeRegistry typeRegistry = new TypeRegistry(typeOperators, new FeaturesConfig());
        InternalTypeManager typeManager = new InternalTypeManager(typeRegistry);
        MetadataManager metadata = new MetadataManager(
                new FeaturesConfig(),
                new DisabledSystemSecurityMetadata(),
                transactionManager,
                user -> ImmutableSet.of(),
                typeOperators,
                new BlockTypeOperators(typeOperators),
                typeManager,
                new InternalBlockEncodingSerde(new BlockEncodingManager(), typeManager),
                NodeVersion.UNKNOWN);

        typeRegistry.addParametricType(new ClassifierParametricType());
        typeRegistry.addType(ModelType.MODEL);
        typeRegistry.addType(RegressorType.REGRESSOR);
        metadata.addFunctions(extractFunctions(new MLPlugin().getFunctions()));
        FUNCTION_RESOLUTION = new TestingFunctionResolution(transactionManager, metadata);
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
