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
import io.trino.tests.product.gcs.GcsTableFormatsTestUtils;
import org.junit.jupiter.api.Test;

@ProductTest
@RequiresEnvironment(GcsEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.IcebergGcs
@TestGroup.ProfileSpecificTests
class TestIcebergGcs
{
    @Test
    void testCreateAndSelectNationTable(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testCreateAndSelectNationTable(env, "iceberg");
    }

    @Test
    void testBasicWriteOperations(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testBasicWriteOperations(env, "iceberg");
    }

    @Test
    void testPathContainsSpecialCharacter(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testPathContainsSpecialCharacter(env, "iceberg", "partitioning", true, "iceberg_test");
    }

    @Test
    void testLocationContainsDiscouragedCharacter(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter(env, "iceberg");
    }

    @Test
    void testSparkReadingTrinoData(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable(env, "iceberg", "iceberg_test");
    }
}
