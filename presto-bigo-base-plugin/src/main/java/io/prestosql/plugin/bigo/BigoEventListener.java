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
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;

/**
 * @author tangyun@bigo.sg
 * @date 7/1/19 8:41 PM
 */
public class BigoEventListener
        implements EventListener
{
    private final KafkaSinker kafkaSinker;

    public BigoEventListener(KafkaSinker kafkaSinker)
    {
        this.kafkaSinker = kafkaSinker;
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        AuditLogBean auditLogBean = new AuditLogBean(queryCompletedEvent);
        kafkaSinker.send(auditLogBean.getQueryId(), auditLogBean.toString());
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }
}
