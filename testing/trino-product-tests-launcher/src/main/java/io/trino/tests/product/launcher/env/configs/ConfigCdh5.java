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
package io.trino.tests.product.launcher.env.configs;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class ConfigCdh5
        extends ConfigDefault
{
    /**
     * export HADOOP_BASE_IMAGE="ghcr.io/trinodb/testing/cdh5.15-hive"
     */
    @Override
    public String getHadoopBaseImage()
    {
        return "ghcr.io/trinodb/testing/cdh5.15-hive";
    }

    @Override
    public List<String> getExcludedTests()
    {
        return ImmutableList.copyOf(new String[] {
                // CDH 5's Avro does not support date type
                "io.trino.tests.product.hive.TestAllDatatypesFromHiveConnector.testSelectAllDatatypesAvro",

                // CDH 5 metastore automatically gathers raw data size statistics on its own
                "io.trino.tests.product.hive.TestHiveBasicTableStatistics.testCreateExternalUnpartitioned",
                "io.trino.tests.product.hive.TestHiveBasicTableStatistics.testAnalyzePartitioned",

                //  CDH 5 image has no lzo support
                "io.trino.tests.product.hive.TestHiveCompression.testReadTextfileWithLzop",
                "io.trino.tests.product.hive.TestHiveCompression.testReadSequencefileWithLzo"
        });
    }
}
