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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.gcs.GcsEnvironment;
import io.trino.tests.product.gcs.GcsTableFormatsTestUtils;
import org.junit.jupiter.api.Test;

@ProductTest
@RequiresEnvironment(GcsEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.HiveGcs
@TestGroup.ProfileSpecificTests
class TestHiveGcs
{
    @Test
    void testInsertTable(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testCreateAndInsertTable(env, "hive");
    }

    @Test
    void testPathContainsSpecialCharacter(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testPathContainsSpecialCharacter(env, "hive", "partitioned_by", true, "spark_catalog");
    }

    @Test
    void testLocationContainsDiscouragedCharacter(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter(env, "hive");
    }

    @Test
    void testSparkReadingTrinoData(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable(env, "hive", "spark_catalog");
    }
}
