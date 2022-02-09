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
package io.trino.plugin.kinesis;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestKinesisConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(KinesisConfig.class)
                .setDefaultSchema("default")
                .setHideInternalColumns(true)
                .setTableDescriptionLocation("etc/kinesis/")
                .setAccessKey(null)
                .setSecretKey(null)
                .setAwsRegion("us-east-1")
                .setTableDescriptionRefreshInterval(new Duration(10, TimeUnit.MINUTES))
                .setSleepTime(new Duration(1000, TimeUnit.MILLISECONDS))
                .setFetchAttempts(2)
                .setMaxBatches(600)
                .setBatchSize(10000)
                .setLogBatches(true)
                .setIteratorFromTimestamp(true)
                .setIteratorOffsetSeconds(86400)
                .setCheckpointEnabled(false)
                .setDynamoReadCapacity(50)
                .setDynamoWriteCapacity(10)
                .setLogicalProcessName("process1")
                .setIteratorNumber(0));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kinesis.table-description-location", "/var/lib/kinesis")
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "false")
                .put("kinesis.access-key", "kinesis.accessKey")
                .put("kinesis.secret-key", "kinesis.secretKey")
                .put("kinesis.fetch-attempts", "3")
                .put("kinesis.max-batches", "500")
                .put("kinesis.aws-region", "us-west-1")
                .put("kinesis.table-description-refresh-interval", "200ms")
                .put("kinesis.sleep-time", "100ms")
                .put("kinesis.batch-size", "9000")
                .put("kinesis.log-batches", "false")
                .put("kinesis.iterator-from-timestamp", "false")
                .put("kinesis.iterator-offset-seconds", "36000")
                .put("kinesis.checkpoint-enabled", "true")
                .put("kinesis.dynamo-read-capacity", "100")
                .put("kinesis.dynamo-write-capacity", "20")
                .put("kinesis.checkpoint-logical-name", "process")
                .put("kinesis.iterator-number", "1")
                .buildOrThrow();

        KinesisConfig expected = new KinesisConfig()
                .setTableDescriptionLocation("/var/lib/kinesis")
                .setDefaultSchema("kinesis")
                .setHideInternalColumns(false)
                .setAccessKey("kinesis.accessKey")
                .setSecretKey("kinesis.secretKey")
                .setAwsRegion("us-west-1")
                .setTableDescriptionRefreshInterval(new Duration(200, TimeUnit.MILLISECONDS))
                .setFetchAttempts(3)
                .setMaxBatches(500)
                .setSleepTime(new Duration(100, TimeUnit.MILLISECONDS))
                .setBatchSize(9000)
                .setLogBatches(false)
                .setIteratorFromTimestamp(false)
                .setIteratorOffsetSeconds(36000)
                .setCheckpointEnabled(true)
                .setDynamoReadCapacity(100)
                .setDynamoWriteCapacity(20)
                .setLogicalProcessName("process")
                .setIteratorNumber(1);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
