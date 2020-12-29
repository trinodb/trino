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
package io.prestosql.tests.hive;

import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.configuration.Configuration;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.prestosql.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.ORDERS;
import static io.prestosql.tests.TestGroups.HIVE_COMPRESSION;
import static io.prestosql.tests.TestGroups.SKIP_ON_CDH;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCompression
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return immutableTable(ORDERS);
    }

    @Test(groups = {HIVE_COMPRESSION, SKIP_ON_CDH /* no lzo support in CDH image */})
    public void testReadLzop()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_read_lzop");

        try {
            onHive().executeQuery("SET hive.exec.compress.output=true");
            onHive().executeQuery("SET mapreduce.output.fileoutputformat.compress=true");
            onHive().executeQuery("SET mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec");
            onHive().executeQuery("CREATE TABLE test_read_lzop STORED AS TEXTFILE AS SELECT * FROM orders");

            assertThat(onPresto().executeQuery("SELECT count(*) FROM test_read_lzop"))
                    .containsExactly(row(1500000));
            assertThat(onPresto().executeQuery("SELECT sum(o_orderkey) FROM test_read_lzop"))
                    .containsExactly(row(4499987250000L));

            assertThat((String) onPresto().executeQuery("SELECT \"$path\" FROM test_read_lzop LIMIT 1").row(0).get(0))
                    .endsWith(".lzo"); // LZOP compression uses .lzo file extension by default
        }
        finally {
            // TODO use inNewConnection instead
            onHive().executeQuery("RESET");

            onHive().executeQuery("DROP TABLE IF EXISTS test_read_lzop");
        }
    }
}
