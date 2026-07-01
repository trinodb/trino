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
package io.trino.tests.product.localfilesystem;

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.LOCAL_FILESYSTEM;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the Hive, Iceberg, and Delta Lake connectors can read and write tables backed by
 * the native local file system ({@code fs.local.enabled=true}) shared across the coordinator and
 * workers, as configured by the {@code multinode-local-filesystem} environment. Schemas are
 * created without an explicit location, exercising the default {@code local://} location handling.
 */
public class TestLocalFilesystem
        extends ProductTest
{
    @Test(groups = {LOCAL_FILESYSTEM, PROFILE_SPECIFIC_TESTS})
    public void testHiveReadWrite()
    {
        testReadWrite("hive");
    }

    @Test(groups = {LOCAL_FILESYSTEM, PROFILE_SPECIFIC_TESTS})
    public void testIcebergReadWrite()
    {
        testReadWrite("iceberg");
    }

    @Test(groups = {LOCAL_FILESYSTEM, PROFILE_SPECIFIC_TESTS})
    public void testDeltaLakeReadWrite()
    {
        testReadWrite("delta");
    }

    private static void testReadWrite(String catalog)
    {
        String schema = catalog + "_local_fs_" + randomNameSuffix();
        String table = format("%s.%s.nation", catalog, schema);
        try {
            onTrino().executeQuery(format("CREATE SCHEMA %s.%s", catalog, schema));
            onTrino().executeQuery(format("CREATE TABLE %s (n_nationkey bigint, n_name varchar)", table));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (1, 'one'), (2, 'two'), (3, 'three')", table));

            assertThat(onTrino().executeQuery(format("SELECT n_nationkey, n_name FROM %s", table)))
                    .containsOnly(row(1, "one"), row(2, "two"), row(3, "three"));
            assertThat(onTrino().executeQuery(format("SELECT count(*) FROM %s", table)))
                    .containsOnly(row(3));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", table));
            onTrino().executeQuery(format("DROP SCHEMA IF EXISTS %s.%s", catalog, schema));
        }
    }
}
