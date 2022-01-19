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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.ml.type.RegressorType;
import io.trino.spi.block.Block;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.ml.type.ClassifierType.BIGINT_CLASSIFIER;
import static io.trino.plugin.ml.type.ClassifierType.VARCHAR_CLASSIFIER;
import static io.trino.plugin.ml.type.RegressorType.REGRESSOR;

public final class MLFunctions
{
    private static final Cache<HashCode, Model> MODEL_CACHE = CacheBuilder.newBuilder().maximumSize(5).build();
    private static final String MAP_BIGINT_DOUBLE = "map(bigint,double)";

    private MLFunctions()
    {
    }

    @ScalarFunction("classify")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice varcharClassify(@SqlType(MAP_BIGINT_DOUBLE) Block featuresMap, @SqlType("Classifier(varchar)") Slice modelSlice)
    {
        FeatureVector features = ModelUtils.toFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model.getType().equals(VARCHAR_CLASSIFIER), "model is not a Classifier(varchar)");
        Classifier<String> varcharClassifier = (Classifier<String>) model;
        return Slices.utf8Slice(varcharClassifier.classify(features));
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long classify(@SqlType(MAP_BIGINT_DOUBLE) Block featuresMap, @SqlType("Classifier(bigint)") Slice modelSlice)
    {
        FeatureVector features = ModelUtils.toFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model.getType().equals(BIGINT_CLASSIFIER), "model is not a Classifier(bigint)");
        Classifier<Integer> classifier = (Classifier<Integer>) model;
        return classifier.classify(features);
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double regress(@SqlType(MAP_BIGINT_DOUBLE) Block featuresMap, @SqlType(RegressorType.NAME) Slice modelSlice)
    {
        FeatureVector features = ModelUtils.toFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model.getType().equals(REGRESSOR), "model is not a regressor");
        Regressor regressor = (Regressor) model;
        return regressor.regress(features);
    }

    private static Model getOrLoadModel(Slice slice)
    {
        HashCode modelHash = ModelUtils.modelHash(slice);
        try {
            return MODEL_CACHE.get(modelHash, () -> ModelUtils.deserialize(slice));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
