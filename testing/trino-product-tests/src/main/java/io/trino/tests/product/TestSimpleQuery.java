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
package io.trino.tests.product;

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.ImmutableTableRequirement;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContextIfSet;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.trino.tests.product.TestGroups.SIMPLE;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimpleQuery
        extends ProductTest
{
    private static class SimpleTestRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return new ImmutableTableRequirement(NATION);
        }
    }

    @BeforeMethodWithContext
    public void beforeTest()
    {
        assertThat(testContextIfSet().isPresent()).isTrue();
    }

    @AfterMethodWithContext
    public void afterTest()
    {
        assertThat(testContextIfSet().isPresent()).isTrue();
    }

    @Test(groups = {SIMPLE, SMOKE})
    @Requires(SimpleTestRequirements.class)
    public void selectAllFromNation()
    {
        assertThat(onTrino().executeQuery("select * from nation")).hasRowsCount(25);
    }

    @Test(groups = {SIMPLE, SMOKE})
    @Requires(SimpleTestRequirements.class)
    public void selectCountFromNation()
    {
        assertThat(onTrino().executeQuery("select count(*) from nation"))
                .hasRowsCount(1)
                .contains(row(25));
    }
}
