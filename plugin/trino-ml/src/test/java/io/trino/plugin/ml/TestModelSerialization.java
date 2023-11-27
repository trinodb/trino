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
import org.junit.jupiter.api.Test;

import static io.trino.plugin.ml.TestUtils.getDataset;
import static org.assertj.core.api.Assertions.assertThat;

public class TestModelSerialization
{
    @Test
    public void testSvmClassifier()
    {
        Model model = new SvmClassifier();
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertThat(deserialized)
                .describedAs("deserialization failed")
                .isNotNull();
        assertThat(deserialized)
                .describedAs("deserialized model is not a svm")
                .isInstanceOf(SvmClassifier.class);
    }

    @Test
    public void testSvmRegressor()
    {
        Model model = new SvmRegressor();
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertThat(deserialized)
                .describedAs("deserialization failed")
                .isNotNull();
        assertThat(deserialized)
                .describedAs("deserialized model is not a svm")
                .isInstanceOf(SvmRegressor.class);
    }

    @Test
    public void testRegressorFeatureTransformer()
    {
        Model model = new RegressorFeatureTransformer(new SvmRegressor(), new FeatureVectorUnitNormalizer());
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertThat(deserialized)
                .describedAs("deserialization failed")
                .isNotNull();
        assertThat(deserialized)
                .describedAs("deserialized model is not a regressor feature transformer")
                .isInstanceOf(RegressorFeatureTransformer.class);
    }

    @Test
    public void testClassifierFeatureTransformer()
    {
        Model model = new ClassifierFeatureTransformer(new SvmClassifier(), new FeatureVectorUnitNormalizer());
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertThat(deserialized)
                .describedAs("deserialization failed")
                .isNotNull();
        assertThat(deserialized)
                .describedAs("deserialized model is not a classifier feature transformer")
                .isInstanceOf(ClassifierFeatureTransformer.class);
    }

    @Test
    public void testVarcharClassifierAdapter()
    {
        Model model = new StringClassifierAdapter(new ClassifierFeatureTransformer(new SvmClassifier(), new FeatureVectorUnitNormalizer()));
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertThat(deserialized)
                .describedAs("deserialization failed")
                .isNotNull();
        assertThat(deserialized)
                .describedAs("deserialized model is not a varchar classifier adapter")
                .isInstanceOf(StringClassifierAdapter.class);
    }

    @Test
    public void testSerializationIds()
    {
        assertThat((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(SvmClassifier.class)).isEqualTo(1);
        assertThat((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(SvmRegressor.class)).isEqualTo(2);
        assertThat((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(FeatureVectorUnitNormalizer.class)).isEqualTo(3);
        assertThat((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(ClassifierFeatureTransformer.class)).isEqualTo(4);
        assertThat((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(RegressorFeatureTransformer.class)).isEqualTo(5);
        assertThat((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(FeatureUnitNormalizer.class)).isEqualTo(6);
        assertThat((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(StringClassifierAdapter.class)).isEqualTo(7);
    }
}
