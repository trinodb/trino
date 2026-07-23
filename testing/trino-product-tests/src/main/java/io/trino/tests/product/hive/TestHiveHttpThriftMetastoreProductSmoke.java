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

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE4_HTTP_THRIFT_METASTORE;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestHiveHttpThriftMetastoreProductSmoke
        extends ProductTest
{
    @Test(groups = {HIVE4_HTTP_THRIFT_METASTORE, PROFILE_SPECIFIC_TESTS})
    public void testSelectFromInformationSchema()
    {
        assertThat(onTrino().executeQuery("SELECT count(*) FROM hive.information_schema.schemata WHERE schema_name = 'default'"))
                .containsOnly(row(1));
    }
}
