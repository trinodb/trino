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
package io.trino.plugin.exchange;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeManagerFactory;
import io.trino.spi.exchange.ExchangeManagerHandleResolver;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FileSystemExchangeManagerFactory
        implements ExchangeManagerFactory
{
    @Override
    public String getName()
    {
        return "filesystem";
    }

    @Override
    public ExchangeManager create(Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(new FileSystemExchangeModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(FileSystemExchangeManager.class);
    }

    @Override
    public ExchangeManagerHandleResolver getHandleResolver()
    {
        return new ExchangeManagerHandleResolver()
        {
            @Override
            public Class<? extends ExchangeSinkInstanceHandle> getExchangeSinkInstanceHandleClass()
            {
                return FileSystemExchangeSinkInstanceHandle.class;
            }

            @Override
            public Class<? extends ExchangeSourceHandle> getExchangeSourceHandleHandleClass()
            {
                return FileSystemExchangeSourceHandle.class;
            }
        };
    }
}
