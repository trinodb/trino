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
package io.trino.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.spi.Page;

import static java.util.Objects.requireNonNull;

public class PassthroughExchanger
        implements LocalExchanger
{
    private final LocalExchangeSource localExchangeSource;
    private final LocalExchangeMemoryManager bufferMemoryManager;

    public PassthroughExchanger(LocalExchangeSource localExchangeSource, LocalExchangeMemoryManager bufferMemoryManager)
    {
        this.localExchangeSource = requireNonNull(localExchangeSource, "localExchangeSource is null");
        this.bufferMemoryManager = requireNonNull(bufferMemoryManager, "bufferMemoryManager is null");
    }

    @Override
    public void accept(Page page)
    {
        bufferMemoryManager.updateMemoryUsage(page.getRetainedSizeInBytes());
        localExchangeSource.addPage(page);
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return bufferMemoryManager.getNotFullFuture();
    }

    @Override
    public void finish()
    {
        localExchangeSource.finish();
    }
}
