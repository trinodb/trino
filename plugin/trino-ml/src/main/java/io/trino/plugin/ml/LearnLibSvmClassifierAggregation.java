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

import io.airlift.slice.Slice;
import io.trino.plugin.ml.type.ClassifierType;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;

import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.VARCHAR;

@AggregationFunction(value = "learn_libsvm_classifier", decomposable = false)
public final class LearnLibSvmClassifierAggregation
{
    private LearnLibSvmClassifierAggregation() {}

    @InputFunction
    @LiteralParameters("x")
    public static void input(
            @AggregationState LearnState state,
            @SqlType(BIGINT) long label,
            @SqlType("map(bigint,double)") Block features,
            @SqlType("varchar(x)") Slice parameters)
    {
        input(state, (double) label, features, parameters);
    }

    @InputFunction
    public static void input(
            @AggregationState LearnState state,
            @SqlType(DOUBLE) double label,
            @SqlType("map(bigint,double)") Block features,
            @SqlType(VARCHAR) Slice parameters)
    {
        state.getLabels().add(label);
        FeatureVector featureVector = ModelUtils.toFeatures(features);
        state.addMemoryUsage(featureVector.getEstimatedSize());
        state.getFeatureVectors().add(featureVector);
        state.setParameters(parameters);
    }

    @OutputFunction("Classifier(bigint)")
    public static void output(@AggregationState LearnState state, BlockBuilder out)
    {
        Dataset dataset = new Dataset(state.getLabels(), state.getFeatureVectors(), state.getLabelEnumeration().inverse());
        Model model = new ClassifierFeatureTransformer(new SvmClassifier(LibSvmUtils.parseParameters(state.getParameters().toStringUtf8())), new FeatureUnitNormalizer());
        model.train(dataset);
        ClassifierType.BIGINT_CLASSIFIER.writeSlice(out, ModelUtils.serialize(model));
    }
}
