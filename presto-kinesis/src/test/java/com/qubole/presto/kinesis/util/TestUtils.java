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
package com.qubole.presto.kinesis.util;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.qubole.presto.kinesis.KinesisClientProvider;
import com.qubole.presto.kinesis.KinesisConnector;
import com.qubole.presto.kinesis.KinesisPlugin;
import com.qubole.presto.kinesis.KinesisStreamDescription;
import com.qubole.presto.kinesis.KinesisStreamFieldDescription;
import com.qubole.presto.kinesis.KinesisStreamFieldGroup;
import com.qubole.presto.kinesis.KinesisTableDescriptionSupplier;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Utilities to help create needed objects to make unit testing possible.
 */
public class TestUtils
{
    /**
     * Use this in POM to avoid providing credentials there (blank is OK for the value but can't be blank in POM).
     */
    public static final String NONE_KEY = "NONE";

    private TestUtils() {}

    /**
     * Create the plug in instance with other dependencies setup.
     * <p>
     * This is done separately so that the injector can be pulled out to get at
     * underlying support classes to test them independently.
     *
     * @return
     */
    public static KinesisPlugin createPluginInstance()
    {
        KinesisPlugin kinesisPlugin = new KinesisPlugin();
        return kinesisPlugin;
    }

    /**
     * Build a connector instance from the plug in, supplying the given properties.
     * <p>
     * This can build a connector with the mock client which is normally done in testing.
     * The plug in is created first with createPluginInstance.
     *
     * @param plugin
     * @param properties
     * @param withMockClient
     * @return
     */
    public static KinesisConnector createConnector(KinesisPlugin plugin, Map<String, String> properties, boolean withMockClient)
    {
        requireNonNull(plugin, "Plugin instance should not be null");
        requireNonNull(properties, "Properties map should not be null (can be empty)");

        if (withMockClient) {
            plugin.setAltProviderClass(KinesisTestClientManager.class);
        }

        ConnectorFactory factory = plugin.getConnectorFactories().iterator().next();
        assertNotNull(factory);

        Connector connector = factory.create("kinesis", properties, new TestingConnectorContext() {});
        assertTrue(connector instanceof KinesisConnector);
        return (KinesisConnector) connector;
    }

    /**
     * Install the plugin into the given query runner, using the mock client and the given table descriptions.
     * <p>
     * The plug in is returned so that the injector can be accessed and other setup items tested.
     *
     * @param queryRunner
     * @param streamDescriptions
     * @return
     */
    public static KinesisPlugin installKinesisPlugin(QueryRunner queryRunner, Map<SchemaTableName, KinesisStreamDescription> streamDescriptions)
    {
        KinesisPlugin kinesisPlugin = createPluginInstance();
        // Note: function literal with provided descriptions instead of KinesisTableDescriptionSupplier:
        kinesisPlugin.setTableDescriptionSupplier(() -> streamDescriptions);
        kinesisPlugin.setAltProviderClass(KinesisTestClientManager.class);

        queryRunner.installPlugin(kinesisPlugin);

        Map<String, String> kinesisConfig = ImmutableMap.of(
                "kinesis.default-schema", "default",
                "kinesis.access-key", "",
                "kinesis.secret-key", "");
        queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);

        return kinesisPlugin;
    }

    /**
     * Install the plug in into the given query runner, using normal setup but with the given table descriptions.
     * <p>
     * Note that this uses the actual client and will incur charges from AWS when run.  Mainly for full
     * integration tests.
     *
     * @param queryRunner
     * @param streamDescriptions
     * @param accessKey
     * @param secretKey
     */
    public static void installKinesisPlugin(QueryRunner queryRunner, Map<SchemaTableName, KinesisStreamDescription> streamDescriptions, String accessKey, String secretKey)
    {
        KinesisPlugin kinesisPlugin = createPluginInstance();
        // Note: function literal with provided descriptions instead of KinesisTableDescriptionSupplier:
        kinesisPlugin.setTableDescriptionSupplier(() -> streamDescriptions);
        queryRunner.installPlugin(kinesisPlugin);

        Map<String, String> kinesisConfig = ImmutableMap.of(
                "kinesis.default-schema", "default",
                "kinesis.access-key", accessKey,
                "kinesis.secret-key", secretKey);
        queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);
    }

    public static Map.Entry<SchemaTableName, KinesisStreamDescription> createEmptyStreamDescription(String streamName, SchemaTableName schemaTableName)
    {
        return new AbstractMap.SimpleImmutableEntry<>(
                schemaTableName,
                new KinesisStreamDescription(schemaTableName.getTableName(), schemaTableName.getSchemaName(), streamName, null));
    }

    public static Map.Entry<SchemaTableName, KinesisStreamDescription> createSimpleJsonStreamDescription(String streamName, SchemaTableName schemaTableName)
    {
        // Format: {"id" : 1324, "name" : "some string"}
        ArrayList<KinesisStreamFieldDescription> fieldList = new ArrayList<KinesisStreamFieldDescription>();
        fieldList.add(new KinesisStreamFieldDescription("id", BigintType.BIGINT, "id", "comment", null, null, false));
        fieldList.add(new KinesisStreamFieldDescription("name", VarcharType.VARCHAR, "name", "comment", null, null, false));
        KinesisStreamFieldGroup grp = new KinesisStreamFieldGroup("json", fieldList);

        KinesisStreamDescription desc = new KinesisStreamDescription(schemaTableName.getTableName(), schemaTableName.getSchemaName(), streamName, grp);
        return new AbstractMap.SimpleImmutableEntry<>(schemaTableName, desc);
    }

    /**
     * Filter out cases where we don't want to explicitly provide credentials and follow the default setup.
     *
     * @param awsValue
     * @return
     */
    public static String noneToBlank(String awsValue)
    {
        if (awsValue.equals(NONE_KEY)) {
            return "";
        }
        else {
            return awsValue;
        }
    }

    /**
     * Convenience method to get the table description supplier.
     *
     * @param inj
     * @return
     */
    public static KinesisTableDescriptionSupplier getTableDescSupplier(Injector inj)
    {
        requireNonNull(inj, "Injector is missing in getTableDescSupplier");
        Supplier<Map<SchemaTableName, KinesisStreamDescription>> supplier =
                inj.getInstance(Key.get(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>() {}));
        requireNonNull(inj, "Injector cannot find any table description supplier");
        return (KinesisTableDescriptionSupplier) supplier;
    }

    /**
     * Convenience method to get the KinesisTestClientManager
     *
     * @param inj
     * @return
     */
    public static KinesisTestClientManager getTestClientManager(Injector inj)
    {
        requireNonNull(inj, "Injector is missing in getTestClientManager");
        KinesisClientProvider prov = inj.getInstance(KinesisClientProvider.class);
        assertTrue(prov instanceof KinesisTestClientManager);
        return (KinesisTestClientManager) prov;
    }
}
