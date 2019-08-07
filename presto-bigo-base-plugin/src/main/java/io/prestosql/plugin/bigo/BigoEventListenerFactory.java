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
package io.prestosql.plugin.bigo;

import io.prestosql.plugin.bigo.kafka.KafkaSinker;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @author tangyun@bigo.sg
 * @date 7/1/19 8:42 PM
 */
@Slf4j
public class BigoEventListenerFactory
        implements EventListenerFactory
{
    private KafkaSinker kafkaSinker;

    public BigoEventListenerFactory()
    {
    }

    @Override
    public String getName()
    {
        return "presto-bigo-base-plugin";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        try {
            log.info("broker.list {}, sinker.topic {}", config.get("broker.list"), config.get("sinker.topic"));
            kafkaSinker = new KafkaSinker(config.get("broker.list"), config.get("sinker.topic"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        kafkaSinker.init();
        return new BigoEventListener(kafkaSinker);
    }
}
