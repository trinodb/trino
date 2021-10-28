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
package io.trino.execution.scheduler;

import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;

import java.util.List;

public class TestingExchangeManager
        implements ExchangeManager
{
    private final boolean splitPartitionsEnabled;

    public TestingExchangeManager(boolean splitPartitionsEnabled)
    {
        this.splitPartitionsEnabled = splitPartitionsEnabled;
    }

    @Override
    public Exchange createExchange(ExchangeContext context, int outputPartitionCount)
    {
        return new TestingExchange(splitPartitionsEnabled);
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExchangeSource createSource(List<ExchangeSourceHandle> handles)
    {
        throw new UnsupportedOperationException();
    }
}
