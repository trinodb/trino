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
package io.trino.server.testing.exchange;

import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.file.Files.createDirectories;
import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeManager
        implements ExchangeManager
{
    private final Path baseDirectory;

    public LocalFileSystemExchangeManager(Path baseDirectory)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
    }

    @Override
    public Exchange createExchange(ExchangeContext context, int outputPartitionCount)
    {
        LocalFileSystemExchange exchange = new LocalFileSystemExchange(baseDirectory, context, outputPartitionCount);
        exchange.initialize();
        return exchange;
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle, boolean preserveRecordsOrder)
    {
        LocalFileSystemExchangeSinkInstanceHandle localHandle = (LocalFileSystemExchangeSinkInstanceHandle) handle;
        Path outputDirectory = localHandle.getOutputDirectory();
        try {
            createDirectories(outputDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new LocalFileSystemExchangeSink(outputDirectory, localHandle.getOutputPartitionCount());
    }

    @Override
    public ExchangeSource createSource(List<ExchangeSourceHandle> handles)
    {
        List<Path> files = handles.stream()
                .map(LocalFileSystemExchangeSourceHandle.class::cast)
                .flatMap(handle -> handle.getFiles().stream())
                .map(Paths::get)
                .collect(toImmutableList());
        return new LocalFileSystemExchangeSource(files);
    }
}
