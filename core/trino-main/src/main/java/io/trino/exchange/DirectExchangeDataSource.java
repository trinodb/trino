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
package io.trino.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.operator.DirectExchangeClient;
import io.trino.operator.OperatorInfo;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class DirectExchangeDataSource
        implements ExchangeDataSource
{
    private final DirectExchangeClient directExchangeClient;

    public DirectExchangeDataSource(DirectExchangeClient directExchangeClient)
    {
        this.directExchangeClient = requireNonNull(directExchangeClient, "directExchangeClient is null");
    }

    @Override
    public Slice pollPage()
    {
        return directExchangeClient.pollPage();
    }

    @Override
    public boolean isFinished()
    {
        return directExchangeClient.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return directExchangeClient.isBlocked();
    }

    @Override
    public void addInput(ExchangeInput input)
    {
        DirectExchangeInput exchangeInput = (DirectExchangeInput) input;
        directExchangeClient.addLocation(exchangeInput.getTaskId(), URI.create(exchangeInput.getLocation()));
    }

    @Override
    public void noMoreInputs()
    {
        directExchangeClient.noMoreLocations();
    }

    @Override
    public OperatorInfo getInfo()
    {
        return directExchangeClient.getStatus();
    }

    @Override
    public void close()
    {
        directExchangeClient.close();
    }
}
