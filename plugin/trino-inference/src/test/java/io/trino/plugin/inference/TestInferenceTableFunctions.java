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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Tests the connector with OpenVINO Model Server. See {@code openvino/generate_models.py} in the test resources for the models.
 */
@Execution(SAME_THREAD)
final class TestInferenceTableFunctions
        extends AbstractTestQueryFramework
{
    private TestingOpenVinoModelServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingOpenVinoModelServer());
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(new InferencePlugin());
        queryRunner.createCatalog("inference", "inference", ImmutableMap.of(
                "inference.endpoint", server.getUri().toString(),
                "inference.max-batch-size", "2"));
        queryRunner.createCatalog("inference_large_batches", "inference", ImmutableMap.of(
                "inference.endpoint", server.getUri().toString(),
                "inference.max-batch-size", "500"));
        return queryRunner;
    }

    @Test
    void testBooleanAndIntegerTensors()
    {
        // the model outputs come first, followed by all input columns
        assertThat(query(
                """
                SELECT *
                FROM TABLE(inference.system.predict(
                    input => TABLE(SELECT * FROM (VALUES (true, 7, 'a'), (false, -2, 'b'), (true, 0, 'c')) t(active, count, tag)),
                    model_name => 'flags'))
                """))
                .matches("VALUES (14, false, true, 7, 'a'), (-4, true, false, -2, 'b'), (0, false, true, 0, 'c')");
    }

    @Test
    void testDoubleAndBigintTensors()
    {
        // the latest version computes price = 1000 * area + 5000 * rooms
        assertThat(query(
                """
                SELECT id, price
                FROM TABLE(inference.system.predict(
                    input => TABLE(SELECT * FROM (VALUES (1, 100e0, BIGINT '3'), (2, 50.5e0, BIGINT '1')) t(id, area, rooms)),
                    model_name => 'house-price'))
                """))
                .matches("VALUES (1, 115000e0), (2, 55500e0)");
    }

    @Test
    void testModelVersion()
    {
        // version 1 computes price = 1000 * area
        assertThat(query(
                """
                SELECT id, price
                FROM TABLE(inference.system.predict(
                    input => TABLE(SELECT * FROM (VALUES (1, 100e0, BIGINT '3'), (2, 50.5e0, BIGINT '1')) t(id, area, rooms)),
                    model_name => 'house-price',
                    model_version => '1'))
                """))
                .matches("VALUES (1, 100000e0), (2, 50500e0)");
    }

    @Test
    void testArrayTensors()
    {
        // one flower of each species from the iris dataset. The input features has shape [-1, 4], and the output probabilities has shape [-1, 3].
        // OpenVINO computes slightly different values on different CPUs, so the probabilities are compared with a tolerance.
        assertThat(query(
                """
                SELECT id, label, zip_with(probabilities, expected, (actual, value) -> abs(actual - value) < 0.01)
                FROM TABLE(inference.system.predict(
                    input => TABLE(
                        SELECT id, ARRAY[sepal_length, sepal_width, petal_length, petal_width] AS features, expected
                        FROM (VALUES
                            (1, REAL '5.1', REAL '3.5', REAL '1.4', REAL '0.2', ARRAY[0.983e0, 0.017e0, 0e0]),
                            (2, REAL '7.0', REAL '3.2', REAL '4.7', REAL '1.4', ARRAY[0.002e0, 0.871e0, 0.127e0]),
                            (3, REAL '6.3', REAL '3.3', REAL '6.0', REAL '2.5', ARRAY[0e0, 0.004e0, 0.996e0]))
                        t(id, sepal_length, sepal_width, petal_length, petal_width, expected)),
                    model_name => 'iris'))
                """))
                .matches(
                        """
                        VALUES
                            (1, BIGINT '0', ARRAY[true, true, true]),
                            (2, BIGINT '1', ARRAY[true, true, true]),
                            (3, BIGINT '2', ARRAY[true, true, true])
                        """);
    }

    @Test
    void testColumnsMatchedCaseInsensitive()
    {
        assertThat(query("SELECT price FROM TABLE(inference.system.predict(input => TABLE(SELECT 1e0 AS \"AREA\", BIGINT '1' AS Rooms), model_name => 'house-price'))"))
                .matches("VALUES 6000e0");
    }

    @Test
    void testNullInputs()
    {
        assertThat(query(
                """
                SELECT id, price
                FROM TABLE(inference.system.predict(
                    input => TABLE(SELECT * FROM (VALUES (1, 100e0, BIGINT '3'), (2, 50e0, NULL), (3, NULL, BIGINT '1')) t(id, area, rooms)),
                    model_name => 'house-price'))
                """))
                .matches("VALUES (1, 115000e0), (2, NULL), (3, NULL)");

        assertThat(query(
                """
                SELECT id, label, probabilities IS NULL
                FROM TABLE(inference.system.predict(
                    input => TABLE(
                        SELECT * FROM (VALUES
                            (1, ARRAY[REAL '5.1', NULL, REAL '1.4', REAL '0.2']),
                            (2, NULL),
                            (3, ARRAY[REAL '5.1', REAL '3.5', REAL '1.4', REAL '0.2']))
                        t(id, features)),
                    model_name => 'iris'))
                """))
                .matches("VALUES (1, CAST(NULL AS bigint), true), (2, NULL, true), (3, BIGINT '0', false)");
    }

    @Test
    void testBatching()
    {
        long requests = server.getInferenceRequests("flags");
        assertThat(query(
                """
                SELECT count(*), sum(doubled)
                FROM TABLE(inference.system.predict(
                    input => TABLE(SELECT * FROM (VALUES (true, 1), (true, 2), (true, 3), (true, 4), (true, 5)) t(active, count)),
                    model_name => 'flags'))
                """))
                .matches("VALUES (BIGINT '5', BIGINT '30')");
        // 5 rows with a maximum batch size of 2
        assertThat(server.getInferenceRequests("flags") - requests).isEqualTo(3);
    }

    @Test
    void testManyBatchesInOnePage()
    {
        // 3000 rows in batches of 2 are more requests than the HTTP client can queue, so the batches of a page must not be sent all at once
        assertThat(query(
                """
                SELECT count(*), sum(doubled)
                FROM TABLE(inference.system.predict(
                    input => TABLE(SELECT true AS active, CAST(x AS integer) AS count FROM UNNEST(sequence(1, 3000)) t(x)),
                    model_name => 'flags'))
                """))
                .matches("VALUES (BIGINT '3000', BIGINT '9003000')");
    }

    @Test
    void testLargeTable()
    {
        // 15000 rows from several splits. Every prediction must be returned with the input row it was computed from.
        // OpenVINO computes FP64 tensors with 32-bit precision, so the predictions are compared with a relative tolerance.
        long requests = server.getInferenceRequests("house-price");
        assertThat(query(
                """
                SELECT count(*), count(DISTINCT orderkey), count_if(abs(price - (1000 * area + 5000 * rooms)) > 1e-6 * (1000 * area + 5000 * rooms))
                FROM TABLE(inference_large_batches.system.predict(
                    input => TABLE(SELECT orderkey, totalprice / 100 AS area, custkey % 7 AS rooms FROM tpch.tiny.orders),
                    model_name => 'house-price'))
                """))
                .matches("VALUES (BIGINT '15000', BIGINT '15000', BIGINT '0')");
        assertThat(server.getInferenceRequests("house-price") - requests).isGreaterThanOrEqualTo(30);
    }

    @Test
    void testWrongColumnType()
    {
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT 100e0 AS area, 3 AS rooms), model_name => 'house-price'))"))
                .failure().hasMessage("Column rooms has type integer, but model house-price expects bigint (INT64 with shape [-1, 1]). Cast the column to the expected type");
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT ARRAY[1e0, 2e0, 3e0, 4e0] AS features), model_name => 'iris'))"))
                .failure().hasMessage("Column features has type array(double), but model iris expects array(real) (FP32 with shape [-1, 4]). Cast the column to the expected type");
    }

    @Test
    void testWrongArrayLength()
    {
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT ARRAY[REAL '1', REAL '2'] AS features), model_name => 'iris'))"))
                .failure().hasMessage("Model iris expects arrays with 4 elements in column features, but got an array with 2 elements");
    }

    @Test
    void testDescribeModel()
    {
        assertThat(query("SELECT * FROM TABLE(inference.system.describe_model(model_name => 'iris'))"))
                .matches(
                        """
                        VALUES
                            (VARCHAR 'input', VARCHAR 'features', VARCHAR 'FP32', ARRAY[BIGINT '-1', 4], VARCHAR 'array(real)'),
                            ('output', 'label', 'INT64', ARRAY[BIGINT '-1'], 'bigint'),
                            ('output', 'probabilities', 'FP32', ARRAY[BIGINT '-1', 3], 'array(real)')
                        """);

        // tensors that the connector does not support have no Trino type
        assertThat(query("SELECT kind, name, shape, trino_type FROM TABLE(inference.system.describe_model(model_name => 'matrix'))"))
                .matches(
                        """
                        VALUES
                            (VARCHAR 'input', VARCHAR 'values', ARRAY[BIGINT '-1', 2, 2], CAST(NULL AS varchar)),
                            ('output', 'same_values', ARRAY[BIGINT '-1', 2, 2], NULL)
                        """);
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT 1 AS x), model_name => 'matrix'))"))
                .failure().hasMessage("Input values of model matrix has the unsupported shape [-1, 2, 2]. The shape of every input and output must have one or two dimensions: [-1] or [-1, 1] for one value per row, [-1, n] for n values per row, or [-1, -1] for any number of values per row. Change the model, or its configuration on the model server");
    }

    @Test
    void testDescribeModelVersion()
    {
        assertThat(query("SELECT name, trino_type FROM TABLE(inference.system.describe_model(model_name => 'house-price', model_version => '1')) WHERE kind = 'input'"))
                .matches("VALUES (VARCHAR 'area', VARCHAR 'double'), ('rooms', 'bigint')");
        assertThat(query("SELECT * FROM TABLE(inference.system.describe_model(model_name => 'house-price', model_version => '9'))"))
                .failure().hasMessageContaining("/v2/models/house-price/versions/9 failed with HTTP status 404");
    }

    @Test
    void testMissingColumn()
    {
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT 100e0 AS area), model_name => 'house-price'))"))
                .failure().hasMessage("Input table does not have a column for the input rooms of model house-price. The input table must have a column with the name of every input of the model: area double, rooms bigint");
    }

    @Test
    void testAmbiguousColumn()
    {
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT true AS active, 1 AS count, 2 AS \"COUNT\"), model_name => 'flags'))"))
                .failure().hasMessage("Input table has more than one column for the input count of model flags, because column names are matched case-insensitively. Rename or remove columns in the input query");
    }

    @Test
    void testNotFiniteValue()
    {
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT ARRAY[CAST(nan() AS real), REAL '1', REAL '1', REAL '1'] AS features), model_name => 'iris'))"))
                .failure().hasMessage("Column features has the value NaN, which cannot be sent to the model server, because JSON does not support NaN and infinite numbers. Remove or replace these values in the input query");
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT infinity() AS area, BIGINT '1' AS rooms), model_name => 'house-price'))"))
                .failure().hasMessage("Column area has the value Infinity, which cannot be sent to the model server, because JSON does not support NaN and infinite numbers. Remove or replace these values in the input query");
    }

    @Test
    void testUnknownModel()
    {
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT 1e0 AS x), model_name => 'unknown'))"))
                .failure().hasMessageMatching("(?s)Request to .*/v2/models/unknown failed with HTTP status 404\\. Check the model_name and model_version arguments, and the inference.endpoint property of the catalog\\. Response: .*");
        assertThat(query("SELECT * FROM TABLE(inference.system.predict(input => TABLE(SELECT 1e0 AS x), model_name => 'house-price', model_version => '9'))"))
                .failure().hasMessageContaining("/v2/models/house-price/versions/9 failed with HTTP status 404");
    }
}
