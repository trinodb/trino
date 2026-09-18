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
package io.trino.tests.product.iceberg;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.gcs.GcsEnvironment;
import org.junit.jupiter.api.Test;

import static io.trino.tests.product.ConfiguredFeatures.assertDefaultConnectors;
import static io.trino.tests.product.TableFormatsTestUtils.verifyBasicWriteOperations;
import static io.trino.tests.product.TableFormatsTestUtils.verifyCreateAndSelectNationTable;
import static io.trino.tests.product.TableFormatsTestUtils.verifyPathContainsSpecialCharacter;
import static io.trino.tests.product.TableFormatsTestUtils.verifySparkCompatibilityOnTrinoCreatedTable;
import static io.trino.tests.product.gcs.GcsTableFormatsTestUtils.verifyLocationContainsDiscouragedCharacter;

@ProductTest
@RequiresEnvironment(GcsEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.IcebergGcs
@TestGroup.ProfileSpecificTests
class TestIcebergGcs
{
    @Test
    void testConfiguredConnectors(GcsEnvironment environment)
    {
        assertDefaultConnectors(environment, "hive", "iceberg", "delta_lake");
    }

    @Test
    void testCreateAndSelectNationTable(GcsEnvironment env)
    {
        verifyCreateAndSelectNationTable(env, "iceberg");
    }

    @Test
    void testBasicWriteOperations(GcsEnvironment env)
    {
        verifyBasicWriteOperations(env, "iceberg");
    }

    @Test
    void testPathContainsSpecialCharacter(GcsEnvironment env)
    {
        verifyPathContainsSpecialCharacter(env, "iceberg", "partitioning", "iceberg_test");
    }

    @Test
    void testLocationContainsDiscouragedCharacter(GcsEnvironment env)
    {
        verifyLocationContainsDiscouragedCharacter(env, "iceberg");
    }

    @Test
    void testSparkReadingTrinoData(GcsEnvironment env)
    {
        verifySparkCompatibilityOnTrinoCreatedTable(env, "iceberg", "iceberg_test");
    }
}
