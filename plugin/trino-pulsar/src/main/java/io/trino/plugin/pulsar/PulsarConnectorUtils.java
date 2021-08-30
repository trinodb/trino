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
package io.trino.plugin.pulsar;

import org.apache.avro.Schema;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.TopicName;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;

/**
 * Util class for Trino Pulsar connector.
 */
public class PulsarConnectorUtils
{
    private PulsarConnectorUtils() {}

    public static Schema parseSchema(String schemaJson)
    {
        Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        return parser.parse(schemaJson);
    }

    public static boolean isPartitionedTopic(TopicName topicName, PulsarConnectorConfig pulsarConnectorConfig) throws PulsarClientException, PulsarAdminException
    {
        try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)) {
            return pulsarAdmin.topics().getPartitionedTopicMetadata(topicName.toString()).partitions > 0;
        }
    }

    public static Properties getProperties(Map<String, String> configMap)
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    public static String rewriteNamespaceDelimiterIfNeeded(String namespace, PulsarConnectorConfig config)
    {
        return config.getNamespaceDelimiterRewriteEnable()
                ? namespace.replace("/", config.getRewriteNamespaceDelimiter())
                : namespace;
    }

    public static String restoreNamespaceDelimiterIfNeeded(String namespace, PulsarConnectorConfig config)
    {
        return config.getNamespaceDelimiterRewriteEnable()
                ? namespace.replace(config.getRewriteNamespaceDelimiter(), "/")
                : namespace;
    }

    public static long roundToTrinoTime(long timestamp)
    {
        Instant.ofEpochMilli(timestamp);
        LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        int roundedNanos = toIntExact(round(date.getNano(), 6));
        LocalDateTime rounded = date
                .withNano(0)
                .plusNanos(roundedNanos);
        long epochMicros = rounded.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND
                + rounded.getNano() / NANOSECONDS_PER_MICROSECOND;
        verify(
                epochMicros == round(epochMicros, 3),
                "Invalid value of epochMicros for precision %s: %s",
                3,
                epochMicros);
        return epochMicros;
    }
}
