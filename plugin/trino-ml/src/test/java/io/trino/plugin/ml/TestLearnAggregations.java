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
import io.trino.metadata.MetadataManager;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.Accumulator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.plugin.ml.type.ClassifierParametricType;
import io.trino.plugin.ml.type.ModelType;
import io.trino.plugin.ml.type.RegressorType;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Random;

import static io.trino.metadata.FunctionExtractor.extractFunctions;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.trino.testing.StructuralTestUtil.mapBlockOf;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestLearnAggregations
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final MetadataManager METADATA = (MetadataManager) FUNCTION_RESOLUTION.getMetadata();

    static {
        METADATA.addParametricType(new ClassifierParametricType());
        METADATA.addType(ModelType.MODEL);
        METADATA.addType(RegressorType.REGRESSOR);
        METADATA.addFunctions(extractFunctions(new MLPlugin().getFunctions()));
    }

    @Test
    public void testLearn()
    {
        TestingAggregationFunction aggregationFunction = FUNCTION_RESOLUTION.getAggregateFunction(
                QualifiedName.of("learn_classifier"),
                fromTypeSignatures(BIGINT.getTypeSignature(), mapType(BIGINT.getTypeSignature(), DOUBLE.getTypeSignature())));
        assertLearnClassifer(aggregationFunction.bind(ImmutableList.of(0, 1), Optional.empty()).createAccumulator());
    }

    @Test
    public void testLearnLibSvm()
    {
        TestingAggregationFunction aggregationFunction = FUNCTION_RESOLUTION.getAggregateFunction(
                QualifiedName.of("learn_libsvm_classifier"),
                fromTypeSignatures(BIGINT.getTypeSignature(), mapType(BIGINT.getTypeSignature(), DOUBLE.getTypeSignature()), VARCHAR.getTypeSignature()));
        assertLearnClassifer(aggregationFunction.bind(ImmutableList.of(0, 1, 2), Optional.empty()).createAccumulator());
    }

    private static void assertLearnClassifer(Accumulator accumulator)
    {
        accumulator.addInput(getPage());
        BlockBuilder finalOut = accumulator.getFinalType().createBlockBuilder(null, 1);
        accumulator.evaluateFinal(finalOut);
        Block block = finalOut.build();
        Slice slice = accumulator.getFinalType().getSlice(block, 0);
        Model deserialized = ModelUtils.deserialize(slice);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof Classifier, "deserialized model is not a classifier");
    }

    private static Page getPage()
    {
        Type mapType = METADATA.getParameterizedType("map", ImmutableList.of(TypeSignatureParameter.typeParameter(BIGINT.getTypeSignature()), TypeSignatureParameter.typeParameter(DOUBLE.getTypeSignature())));
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
