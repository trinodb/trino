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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import org.testng.annotations.Test;

import javax.validation.constraints.AssertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.testng.Assert.assertEquals;

public class TestMongoClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MongoClientConfig.class)
                .setConnectionUrl(null)
                .setSchemaCollection("_schema")
                .setCaseInsensitiveNameMatching(false)
                .setMinConnectionsPerHost(0)
                .setConnectionsPerHost(100)
                .setMaxWaitTime(120_000)
                .setConnectionTimeout(10_000)
                .setSocketTimeout(0)
                .setTlsEnabled(false)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTruststorePath(null)
                .setTruststorePassword(null)
                .setMaxConnectionIdleTime(0)
                .setCursorBatchSize(0)
                .setReadPreference(ReadPreferenceType.PRIMARY)
                .setWriteConcern(WriteConcernType.ACKNOWLEDGED)
                .setRequiredReplicaSetName(null)
                .setImplicitRowFieldPrefix("_pos"));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("mongodb.schema-collection", "_my_schema")
                .put("mongodb.case-insensitive-name-matching", "true")
                .put("mongodb.connection-url", "mongodb://router1.example.com:27017,router2.example2.com:27017,router3.example3.com:27017/")
                .put("mongodb.min-connections-per-host", "1")
                .put("mongodb.connections-per-host", "99")
                .put("mongodb.max-wait-time", "120001")
                .put("mongodb.connection-timeout", "9999")
                .put("mongodb.socket-timeout", "1")
                .put("mongodb.tls.enabled", "true")
                .put("mongodb.tls.keystore-path", keystoreFile.toString())
                .put("mongodb.tls.keystore-password", "keystore-password")
                .put("mongodb.tls.truststore-path", truststoreFile.toString())
                .put("mongodb.tls.truststore-password", "truststore-password")
                .put("mongodb.max-connection-idle-time", "180000")
                .put("mongodb.cursor-batch-size", "1")
                .put("mongodb.read-preference", "NEAREST")
                .put("mongodb.write-concern", "UNACKNOWLEDGED")
                .put("mongodb.required-replica-set", "replica_set")
                .put("mongodb.implicit-row-field-prefix", "_prefix")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        MongoClientConfig config = configurationFactory.build(MongoClientConfig.class);

        MongoClientConfig expected = new MongoClientConfig()
                .setSchemaCollection("_my_schema")
                .setCaseInsensitiveNameMatching(true)
                .setConnectionUrl("mongodb://router1.example.com:27017,router2.example2.com:27017,router3.example3.com:27017/")
                .setMinConnectionsPerHost(1)
                .setConnectionsPerHost(99)
                .setMaxWaitTime(120_001)
                .setConnectionTimeout(9_999)
                .setSocketTimeout(1)
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password")
                .setMaxConnectionIdleTime(180_000)
                .setCursorBatchSize(1)
                .setReadPreference(ReadPreferenceType.NEAREST)
                .setWriteConcern(WriteConcernType.UNACKNOWLEDGED)
                .setRequiredReplicaSetName("replica_set")
                .setImplicitRowFieldPrefix("_prefix");

        assertEquals(config.getSchemaCollection(), expected.getSchemaCollection());
        assertEquals(config.isCaseInsensitiveNameMatching(), expected.isCaseInsensitiveNameMatching());
        assertEquals(config.getConnectionUrl(), expected.getConnectionUrl());
        assertEquals(config.getMinConnectionsPerHost(), expected.getMinConnectionsPerHost());
        assertEquals(config.getConnectionsPerHost(), expected.getConnectionsPerHost());
        assertEquals(config.getMaxWaitTime(), expected.getMaxWaitTime());
        assertEquals(config.getConnectionTimeout(), expected.getConnectionTimeout());
        assertEquals(config.getSocketTimeout(), expected.getSocketTimeout());
        assertEquals(config.getTlsEnabled(), expected.getTlsEnabled());
        assertEquals(config.getKeystorePath(), expected.getKeystorePath());
        assertEquals(config.getKeystorePassword(), expected.getKeystorePassword());
        assertEquals(config.getTruststorePath(), expected.getTruststorePath());
        assertEquals(config.getTruststorePassword(), expected.getTruststorePassword());
        assertEquals(config.getMaxConnectionIdleTime(), expected.getMaxConnectionIdleTime());
        assertEquals(config.getCursorBatchSize(), expected.getCursorBatchSize());
        assertEquals(config.getReadPreference(), expected.getReadPreference());
        assertEquals(config.getWriteConcern(), expected.getWriteConcern());
        assertEquals(config.getRequiredReplicaSetName(), expected.getRequiredReplicaSetName());
        assertEquals(config.getImplicitRowFieldPrefix(), expected.getImplicitRowFieldPrefix());
    }

    @Test
    public void testValidation()
            throws Exception
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        assertFailsTlsValidation(new MongoClientConfig().setKeystorePath(keystoreFile.toFile()));
        assertFailsTlsValidation(new MongoClientConfig().setKeystorePassword("keystore password"));
        assertFailsTlsValidation(new MongoClientConfig().setTruststorePath(truststoreFile.toFile()));
        assertFailsTlsValidation(new MongoClientConfig().setTruststorePassword("truststore password"));
    }

    private static void assertFailsTlsValidation(MongoClientConfig config)
    {
        assertFailsValidation(
                config,
                "validTlsConfig",
                "'mongodb.tls.keystore-path', 'mongodb.tls.keystore-password', 'mongodb.tls.truststore-path' and 'mongodb.tls.truststore-password' must be empty when TLS is disabled",
                AssertTrue.class);
    }
}
