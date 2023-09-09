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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.Session;
import io.trino.plugin.kafka.schema.confluent.KafkaWithConfluentSchemaRegistryQueryRunner;
import io.trino.spi.type.SqlVarbinary;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.kafka.TestingKafka;
import io.trino.testng.services.ManageTestResources;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.testng.SkipException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public abstract class BaseKafkaWithConfluentLatestSmokeTest
        extends BaseConnectorSmokeTest
{
    @ManageTestResources.Suppress(because = "This class is stateless.")
    private static final RetryPolicy<Object> RETRY_POLICY = RetryPolicy.builder()
            .withMaxAttempts(10)
            .withDelay(Duration.ofMillis(100))
            .build();
    private TestingKafka testingKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        return KafkaWithConfluentSchemaRegistryQueryRunner.builder(testingKafka)
                .setTables(REQUIRED_TPCH_TABLES)
                .setExtraKafkaProperties(ImmutableMap.<String, String>builder()
                        .put("kafka.confluent-subjects-cache-refresh-interval", "1ms")
                        .buildOrThrow())
                .build();
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_MERGE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_CREATE_VIEW:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    protected TestingKafka getTestingKafka()
    {
        return testingKafka;
    }

    @Override
    public void testInsert()
    {
        assertThatThrownBy(super::testInsert)
                .hasMessage("Cannot test INSERT without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        // TODO implement the test for Kafka
        throw new SkipException("Insert tests do not work without create table");
    }

    protected static ImmutableMap.Builder<String, String> schemaRegistryAwareProducer(TestingKafka testingKafka)
    {
        return ImmutableMap.<String, String>builder()
                .put(SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString());
    }

    protected static String toDoubleQuoted(String tableName)
    {
        return format("\"%s\"", tableName);
    }

    protected String toSingleQuotedOrNullLiteral(Object value)
    {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof SqlVarbinary) {
            return "X'" + BaseEncoding.base16().encode(((SqlVarbinary) value).getBytes()) + "'";
        }
        else if (value instanceof byte[]) {
            return "X'" + BaseEncoding.base16().encode(deserializeBytes((byte[]) value)) + "'";
        }
        return "'" + value + "'";
    }

    protected byte[] deserializeBytes(byte[] bytes)
    {
        // Varbinary need to be deserialized using avro decoder.
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(bytes), null);
        try {
            return decoder.readBytes(null).array();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void waitUntilTableExists(String tableName)
    {
        waitUntilTableExists(getQueryRunner().getDefaultSession(), tableName);
    }

    protected void waitUntilTableExists(Session session, String tableName)
    {
        Failsafe.with(RETRY_POLICY)
                .run(() -> assertTrue(schemaExists()));
        Failsafe.with(RETRY_POLICY)
                .run(() -> assertTrue(tableExists(session, tableName)));
    }

    protected boolean schemaExists()
    {
        return getQueryRunner().execute(format(
                        "SHOW SCHEMAS FROM %s LIKE '%s'",
                        getSession().getCatalog().orElseThrow(),
                        getSession().getSchema().orElseThrow()))
                .getRowCount() == 1;
    }

    protected boolean tableExists(Session session, String tableName)
    {
        return getQueryRunner().execute(session, format("SHOW TABLES LIKE '%s'", tableName.toLowerCase(ENGLISH))).getRowCount() == 1;
    }
}
