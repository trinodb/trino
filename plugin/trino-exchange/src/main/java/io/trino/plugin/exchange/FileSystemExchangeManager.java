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

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class FileSystemExchangeManager
        implements ExchangeManager
{
    public static final String PATH_SEPARATOR = "/";

    private static final int KEY_BITS = 256;

    private final FileSystemExchangeStorage exchangeStorage;
    private final URI baseDirectory;
    private final boolean exchangeEncryptionEnabled;
    private final int maxPageStorageSize;
    private final int exchangeSinkBufferPoolMinSize;
    private final ExecutorService executor;

    @Inject
    public FileSystemExchangeManager(FileSystemExchangeStorage exchangeStorage, FileSystemExchangeConfig fileSystemExchangeConfig)
    {
        requireNonNull(fileSystemExchangeConfig, "fileSystemExchangeConfig is null");

        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        String baseDirectory = requireNonNull(fileSystemExchangeConfig.getBaseDirectory(), "baseDirectory is null");
        if (!baseDirectory.endsWith(PATH_SEPARATOR)) {
            // This is needed as URI's resolve method expects directories to end with '/'
            baseDirectory += PATH_SEPARATOR;
        }
        this.baseDirectory = URI.create(baseDirectory);
        this.exchangeEncryptionEnabled = fileSystemExchangeConfig.isExchangeEncryptionEnabled();
        this.maxPageStorageSize = toIntExact(fileSystemExchangeConfig.getMaxPageStorageSize().toBytes());
        this.exchangeSinkBufferPoolMinSize = fileSystemExchangeConfig.getExchangeSinkBufferPoolMinSize();
        this.executor = newCachedThreadPool(daemonThreadsNamed("exchange-source-handles-creation-%s"));
    }

    @Override
    public Exchange createExchange(ExchangeContext context, int outputPartitionCount)
    {
        Optional<SecretKey> secretKey = Optional.empty();
        if (exchangeEncryptionEnabled) {
            try {
                KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
                keyGenerator.init(KEY_BITS);
                secretKey = Optional.of(keyGenerator.generateKey());
            }
            catch (NoSuchAlgorithmException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to generate new secret key: " + e.getMessage(), e);
            }
        }
        FileSystemExchange exchange = new FileSystemExchange(baseDirectory, exchangeStorage, context, outputPartitionCount, secretKey, executor);
        exchange.initialize();
        return exchange;
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle, boolean preserveRecordsOrder)
    {
        FileSystemExchangeSinkInstanceHandle instanceHandle = (FileSystemExchangeSinkInstanceHandle) handle;
        return new FileSystemExchangeSink(
                exchangeStorage,
                instanceHandle.getOutputDirectory(),
                instanceHandle.getOutputPartitionCount(),
                instanceHandle.getSinkHandle().getSecretKey().map(key -> new SecretKeySpec(key, 0, key.length, "AES")),
                maxPageStorageSize,
                exchangeSinkBufferPoolMinSize);
    }

    @Override
    public ExchangeSource createSource(List<ExchangeSourceHandle> handles)
    {
        List<URI> files = handles.stream()
                .map(FileSystemExchangeSourceHandle.class::cast)
                .flatMap(handle -> handle.getFiles().stream())
                .map(fileStatus -> URI.create(fileStatus.getFilePath()))
                .collect(toImmutableList());
        ImmutableList.Builder<Optional<SecretKey>> secretKeys = ImmutableList.builder();
        for (ExchangeSourceHandle handle : handles) {
            FileSystemExchangeSourceHandle sourceHandle = (FileSystemExchangeSourceHandle) handle;
            secretKeys.addAll(nCopies(sourceHandle.getFiles().size(), sourceHandle.getSecretKey().map(key -> new SecretKeySpec(key, 0, key.length, "AES"))));
        }
        return new FileSystemExchangeSource(exchangeStorage, files, secretKeys.build());
    }
}
