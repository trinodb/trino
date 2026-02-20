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

import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.Requires;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableRegionTable;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.HIVE_ICEBERG_REDIRECTIONS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;

@Requires(ImmutableRegionTable.class)
public class TestHiveIcebergViewTranslationLegacy
        extends AbstractTestHiveIcebergViewTranslation
{
    @BeforeMethodWithContext
    public void setup()
    {
        setSessionProperty("hive.hive_views_legacy_translation", "true");
    }

    @Override
    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testViewReferencingHiveAndIcebergTables()
    {
        super.testViewReferencingHiveAndIcebergTables();
    }
}
