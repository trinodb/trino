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
package io.trino.plugin.kinesis.util;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.kinesis.KinesisConnector;
import io.trino.plugin.kinesis.KinesisConnectorFactory;
import io.trino.plugin.kinesis.KinesisPlugin;
import io.trino.plugin.kinesis.TestingKinesisConnectorFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorContext;

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertNotNull;

public final class TestUtils
{
    private static final String NONE_KEY = "NONE";

    private TestUtils() {}

    public static KinesisConnector createConnector(KinesisPlugin plugin, Map<String, String> properties, boolean withMockClient)
    {
        requireNonNull(plugin, "Plugin instance should not be null");
        requireNonNull(properties, "Properties map should not be null (can be empty)");
        ConnectorFactory factory = plugin.getConnectorFactories().iterator().next();
        assertNotNull(factory);

        Connector connector = factory.create("kinesis", properties, new TestingConnectorContext());
        return (KinesisConnector) connector;
    }

    /**
     * Install the plugin into the given query runner, using the mock client and the given table descriptions.
     */
    public static MockKinesisClient installKinesisPlugin(QueryRunner queryRunner)
    {
        KinesisTestClientManager kinesisTestClientManager = new KinesisTestClientManager();
        MockKinesisClient mockClient = (MockKinesisClient) kinesisTestClientManager.getClient();
        mockClient.createStream("test123", 2);
        mockClient.createStream("sampleTable", 2);
        mockClient.createStream("sampleGzipCompressTable", 2);
        mockClient.createStream("sampleAutomaticCompressTable", 2);
        KinesisConnectorFactory kinesisConnectorFactory = new TestingKinesisConnectorFactory(kinesisTestClientManager);

        KinesisPlugin kinesisPlugin = new KinesisPlugin(kinesisConnectorFactory);
        queryRunner.installPlugin(kinesisPlugin);

        Map<String, String> kinesisConfig = ImmutableMap.of(
                "kinesis.default-schema", "default",
                "kinesis.access-key", "",
                "kinesis.secret-key", "",
                "kinesis.table-description-location", "src/test/resources/tableDescriptions");
        queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);

        return mockClient;
    }

    /**
     * Install the plug in into the given query runner, using normal setup but with the given table descriptions.
     * <p>
     * Note that this uses the actual client and will incur charges from AWS when run.  Mainly for full
     * integration tests.
     */
    public static void installKinesisPlugin(QueryRunner queryRunner, String tableDescriptionLocation, String accessKey, String secretKey)
    {
        KinesisPlugin kinesisPlugin = new KinesisPlugin();
        queryRunner.installPlugin(kinesisPlugin);

        Map<String, String> kinesisConfig = ImmutableMap.of(
                "kinesis.default-schema", "default",
                "kinesis.access-key", accessKey,
                "kinesis.secret-key", secretKey,
                "kinesis.table-description-location", tableDescriptionLocation);

        queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);
    }

    public static String noneToBlank(String awsValue)
    {
        if (awsValue.equals(NONE_KEY)) {
            return "";
        }
        return awsValue;
    }
}
