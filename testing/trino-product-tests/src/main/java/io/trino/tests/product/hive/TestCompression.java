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
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.ORDERS;
import static io.trino.tests.product.TestGroups.HIVE_COMPRESSION;
import static io.trino.tests.product.TestGroups.SKIP_ON_CDH;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
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
    public void testReadTextfileWithLzop()
    {
        testReadCompressedTable(
                "STORED AS TEXTFILE",
                "com.hadoop.compression.lzo.LzopCodec",
                ".*\\.lzo"); // LZOP compression uses .lzo file extension by default
    }

    @Test(groups = {HIVE_COMPRESSION, SKIP_ON_CDH /* no lzo support in CDH image */})
    public void testReadSequencefileWithLzo()
    {
        testReadCompressedTable(
                "STORED AS SEQUENCEFILE",
                "com.hadoop.compression.lzo.LzoCodec",
                // sequencefile stores compression information in the file header, so no suffix expected
                "\\d+_0");
    }

    private void testReadCompressedTable(String tableStorageDefinition, String compressionCodec, @Language("RegExp") String expectedFileNamePattern)
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_read_compressed");

        try {
            onHive().executeQuery("SET hive.exec.compress.output=true");
            onHive().executeQuery("SET mapreduce.output.fileoutputformat.compress=true");
            onHive().executeQuery("SET mapreduce.output.fileoutputformat.compress.codec=" + compressionCodec);
            onHive().executeQuery("CREATE TABLE test_read_compressed " + tableStorageDefinition + " AS SELECT * FROM orders");

            assertThat(onTrino().executeQuery("SELECT count(*) FROM test_read_compressed"))
                    .containsExactly(row(1500000));
            assertThat(onTrino().executeQuery("SELECT sum(o_orderkey) FROM test_read_compressed"))
                    .containsExactly(row(4499987250000L));

            assertThat((String) onTrino().executeQuery("SELECT regexp_replace(\"$path\", '.*/') FROM test_read_compressed LIMIT 1").row(0).get(0))
                    .matches(expectedFileNamePattern);
        }
        finally {
            // TODO use inNewConnection instead
            onHive().executeQuery("RESET");

            onHive().executeQuery("DROP TABLE IF EXISTS test_read_compressed");
        }
    }
}
