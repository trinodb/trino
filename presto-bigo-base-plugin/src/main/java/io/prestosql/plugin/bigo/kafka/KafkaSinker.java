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
package io.prestosql.plugin.bigo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class KafkaSinker
{
    private static KafkaProducer<String, String> producer;
    private String brokerlist;
    private String topic;

    public void init()
    {
        if (null == producer) {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", brokerlist);
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(properties);
        }
    }

    public KafkaSinker(String brokerlist, String topic) throws Exception
    {
        if (brokerlist == null || topic == null) {
            throw new Exception("brokerlist and topic cannot be null");
        }

        this.brokerlist = brokerlist;
        this.topic = topic;
    }

    public void send(String key, String message)
    {
        if (key == null || message.length() == 0) {
            return;
        }

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, key, message);
        producer.send(producerRecord);
        new Thread(() -> flush()).start();
    }

    public void flush()
    {
        producer.flush();
    }
}
