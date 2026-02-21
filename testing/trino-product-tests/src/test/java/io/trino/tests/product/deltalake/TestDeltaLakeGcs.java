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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.gcs.GcsEnvironment;
import io.trino.tests.product.gcs.GcsTableFormatsTestUtils;
import org.junit.jupiter.api.Test;

@ProductTest
@RequiresEnvironment(GcsEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeGcs
@TestGroup.ProfileSpecificTests
class TestDeltaLakeGcs
{
    @Test
    void testCreateAndSelectNationTable(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testCreateAndSelectNationTable(env, "delta");
    }

    @Test
    void testBasicWriteOperations(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testBasicWriteOperations(env, "delta");
    }

    @Test
    void testPathContainsSpecialCharacter(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testPathContainsSpecialCharacter(env, "delta", "partitioned_by", false, "spark_catalog");
    }

    @Test
    void testLocationContainsDiscouragedCharacter(GcsEnvironment env)
    {
        GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter(env, "delta");
    }
}
