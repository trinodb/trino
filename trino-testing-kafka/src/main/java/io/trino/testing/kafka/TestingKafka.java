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
package io.trino.testing.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.Closeable;
import java.util.Map;

public interface TestingKafka
        extends Closeable
{
    String DEFAULT_CONFLUENT_PLATFORM_VERSION = "5.5.2";

    void start();

    void createTopic(String topic);

    void createTopicWithConfig(int partitions, int replication, String topic, boolean enableLogAppendTime);

    String getConnectString();

    default <K, V> KafkaProducer<K, V> createProducer()
    {
        return createProducer(ImmutableMap.of());
    }

    <K, V> KafkaProducer<K, V> createProducer(Map<String, String> extraProperties);
}
