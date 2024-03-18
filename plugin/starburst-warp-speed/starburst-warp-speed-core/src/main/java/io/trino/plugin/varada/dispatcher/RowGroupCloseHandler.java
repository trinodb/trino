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
package io.trino.plugin.varada.dispatcher;

import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class RowGroupCloseHandler
        implements Consumer<RowGroupData>
{
    private static final Logger logger = Logger.get(RowGroupCloseHandler.class);

    private final AtomicBoolean handled = new AtomicBoolean();

    @Override
    public void accept(RowGroupData rowGroupData)
    {
        if (!handled.getAndSet(true)) {
            rowGroupData.getLock().readUnLock();
        }
        else {
            logger.warn("already called & handled by this close handled for rowGroup[%s]", rowGroupData.getRowGroupKey());
        }
    }
}
