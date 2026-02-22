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
import io.trino.tests.product.azure.AzureEnvironment;
import io.trino.tests.product.azure.AzureTableFormatsTestUtils;
import org.junit.jupiter.api.Test;

@ProductTest
@RequiresEnvironment(AzureEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeAzure
@TestGroup.ProfileSpecificTests
class TestDeltaLakeAzure
{
    @Test
    void testCreateAndSelectNationTable(AzureEnvironment env)
    {
        AzureTableFormatsTestUtils.testCreateAndSelectNationTable(env, "delta");
    }

    @Test
    void testBasicWriteOperations(AzureEnvironment env)
    {
        AzureTableFormatsTestUtils.testBasicWriteOperations(env, "delta");
    }

    @Test
    void testPathContainsSpecialCharacter(AzureEnvironment env)
    {
        AzureTableFormatsTestUtils.testPathContainsSpecialCharacter(env, "delta", "partitioned_by", false, "spark_catalog");
    }
}
