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

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.SAP_HANA;
import static io.trino.tests.product.TpchTableResults.PRESTO_NATION_RESULT;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSapHana
        extends ProductTest
{
    @Test(groups = {SAP_HANA, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        assertThat(onTrino().executeQuery("CREATE TABLE saphana.system.nation AS SELECT * FROM tpch.tiny.nation"))
                .containsOnly(row(25));
        assertThat(onTrino().executeQuery("SELECT nationkey, name, regionkey, comment FROM saphana.system.nation"))
                .matches(PRESTO_NATION_RESULT);
    }
}
