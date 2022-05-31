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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;

public class TestDeltaLakeOptimizedWriterConnectorSmokeTest
        extends BaseDeltaLakeAwsConnectorSmokeTest
{
    @Override
    QueryRunner createDeltaLakeQueryRunner(Map<String, String> connectorProperties)
            throws Exception
    {
        return DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                ImmutableMap.<String, String>builder()
                        .putAll(connectorProperties)
                        .put("parquet.experimental-optimized-writer.enabled", "true")
                        .put("delta.enable-non-concurrent-writes", "true")
                        .put("hive.s3.max-connections", "2")
                        .buildOrThrow(),
                dockerizedMinioDataLake.getMinioAddress(),
                dockerizedMinioDataLake.getTestingHadoop());
    }
}
