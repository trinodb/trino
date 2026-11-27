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
package io.trino.plugin.inference;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the connector with MLServer, which has model metadata only when the model settings declare it, and whose scikit-learn runtime
 * returns only the output {@code predict} unless the request names the outputs. As the model settings declare the metadata, the tests also use
 * metadata that does not match the model. See {@code mlserver/create_models.py} in the test resources for the models.
 */
final class TestMlServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingMlServer server = closeAfterClass(new TestingMlServer());
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new InferencePlugin());
        queryRunner.createCatalog("mlserver", "inference", ImmutableMap.of(
                "inference.endpoint", server.getUri().toString()));
        return queryRunner;
    }

    @Test
    void testInputsDecodedAsDataFrame()
    {
        // the model was trained on a DataFrame with the columns area and rooms. The linear regression is not exact, so the predictions are rounded
        assertThat(query(
                """
                SELECT id, round(predict)
                FROM TABLE(mlserver.system.predict(
                    input => TABLE(SELECT * FROM (VALUES (1, 100e0, BIGINT '3'), (2, 200e0, BIGINT '2')) t(id, area, rooms)),
                    model_name => 'house-price'))
                """))
                .matches("VALUES (1, 115000e0), (2, 210000e0)");
    }

    @Test
    void testMultipleOutputs()
    {
        // one setosa flower, and one virginica flower from the iris dataset
        assertThat(query(
                """
                SELECT id, predict, cardinality(predict_proba), predict_proba[predict + 1] > 0.5
                FROM TABLE(mlserver.system.predict(
                    input => TABLE(SELECT * FROM (VALUES (1, ARRAY[5.1e0, 3.5e0, 1.4e0, 0.2e0]), (2, ARRAY[6.7e0, 3.0e0, 5.2e0, 2.3e0])) t(id, features)),
                    model_name => 'iris'))
                """))
                .matches("VALUES (1, BIGINT '0', BIGINT '3', true), (2, BIGINT '2', BIGINT '3', true)");
    }

    @Test
    void testSingleOutputOtherThanDefault()
    {
        // the metadata declares only predict_proba, which the runtime returns only when the request names it
        assertThat(query(
                """
                SELECT id, cardinality(predict_proba), predict_proba[1] > 0.5
                FROM TABLE(mlserver.system.predict(
                    input => TABLE(SELECT * FROM (VALUES (1, ARRAY[5.1e0, 3.5e0, 1.4e0, 0.2e0]), (2, ARRAY[6.7e0, 3.0e0, 5.2e0, 2.3e0])) t(id, features)),
                    model_name => 'iris-probabilities'))
                """))
                .matches("VALUES (1, BIGINT '3', true), (2, BIGINT '3', false)");
    }

    @Test
    void testVariableLengthArrays()
    {
        // the model accepts any number of features, as long as the rows of a request have the same number
        assertThat(query(
                """
                SELECT id, predict
                FROM TABLE(mlserver.system.predict(
                    input => TABLE(SELECT * FROM (VALUES (1, ARRAY[5.1e0, 3.5e0, 1.4e0, 0.2e0]), (2, ARRAY[6.7e0, 3.0e0, 5.2e0, 2.3e0])) t(id, features)),
                    model_name => 'iris-variable-length'))
                """))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '2')");
        assertThat(query("SELECT * FROM TABLE(mlserver.system.predict(input => TABLE(SELECT * FROM (VALUES ARRAY[5.1e0, 3.5e0, 1.4e0, 0.2e0], ARRAY[6.7e0, 3.0e0, 5.2e0]) t(features)), model_name => 'iris-variable-length'))"))
                .failure().hasMessage("Model iris-variable-length expects arrays with the same number of elements in column features for all rows that are sent in one request, but got arrays with 4 and 3 elements");
    }

    @Test
    void testUndeclaredMetadata()
    {
        assertThat(query("SELECT * FROM TABLE(mlserver.system.predict(input => TABLE(SELECT 1e0 AS area, BIGINT '1' AS rooms), model_name => 'undeclared-metadata'))"))
                .failure().hasMessage("Metadata of model undeclared-metadata does not declare any inputs. Declare the name, data type, and shape of every input and output in the configuration of the model on the model server");
    }

    @Test
    void testUnsupportedMetadata()
    {
        assertThat(query("SELECT * FROM TABLE(mlserver.system.predict(input => TABLE(SELECT ARRAY[5.1e0, 3.5e0, 1.4e0, 0.2e0] AS features), model_name => 'fixed-rows'))"))
                .failure().hasMessage("Input features of model fixed-rows has the unsupported shape [1, 4]. The first dimension of the shape of every input and output must be -1, for a variable number of rows. Change the model, or its configuration on the model server");
        assertThat(query("SELECT * FROM TABLE(mlserver.system.predict(input => TABLE(SELECT ARRAY[5.1e0, 3.5e0, 1.4e0, 0.2e0] AS features), model_name => 'unsupported-data-type'))"))
                .failure().hasMessage("Output predict of model unsupported-data-type has the unsupported data type UINT64. The data type of every input and output must be one of BOOL, INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, FP16, FP32, FP64, BYTES. Change the model, or its configuration on the model server");
    }

    @Test
    void testMetadataNotMatchingModel()
    {
        assertThat(query("SELECT * FROM TABLE(mlserver.system.predict(input => TABLE(SELECT * FROM (VALUES ARRAY[5.1e0, 3.5e0, 1.4e0, 0.2e0], ARRAY[6.7e0, 3.0e0, 5.2e0, 2.3e0]) t(features)), model_name => 'wrong-output-shape'))"))
                .failure().hasMessage("Model wrong-output-shape returned the output predict_proba with the shape [2, 3] for 2 rows, but the shape [-1, 2] of the output in the model metadata requires 2 values per row. Declare the shape of the output that the model returns in the configuration of the model on the model server");
        assertThat(query("SELECT * FROM TABLE(mlserver.system.predict(input => TABLE(SELECT 100e0 AS area, BIGINT '3' AS rooms), model_name => 'wrong-output-data-type'))"))
                .failure().hasMessageMatching("Model server returned the floating-point number [0-9.E]+ for the output predict of model wrong-output-data-type, but the data type INT64 of the output requires integers. Declare the data type of the output that the model returns in the configuration of the model on the model server");
    }
}
