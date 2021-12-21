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
import javax.inject.Inject;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeManager
        implements ExchangeManager
{
    public static final String AES = "AES";
    public static final int KEY_BITS = 256;
    public static final String PATH_SEPARATOR = "/";

    private final FileSystemExchangeStorage exchangeStorage;
    private final URI baseDirectory;
    private final boolean exchangeEncryptionEnabled;

    @Inject
    public FileSystemExchangeManager(FileSystemExchangeStorage exchangeStorage, FileSystemExchangeConfig fileSystemExchangeConfig)
    {
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        String baseDirectory = requireNonNull(fileSystemExchangeConfig.getBaseDirectory(), "baseDirectory is null");
        if (!baseDirectory.endsWith(PATH_SEPARATOR)) {
            // This is needed as URI's resolve method expects directories to end with '/'
            baseDirectory += PATH_SEPARATOR;
        }
        this.baseDirectory = URI.create(baseDirectory);
        this.exchangeEncryptionEnabled = fileSystemExchangeConfig.isExchangeEncryptionEnabled();
    }

    @Override
    public Exchange create(ExchangeContext context, int outputPartitionCount)
    {
        Optional<SecretKey> secretKey = Optional.empty();
        if (exchangeEncryptionEnabled) {
            try {
                KeyGenerator keyGenerator = KeyGenerator.getInstance(AES);
                keyGenerator.init(KEY_BITS);
                secretKey = Optional.of(keyGenerator.generateKey());
            }
            catch (NoSuchAlgorithmException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to generate new secret key: " + e.getMessage(), e);
            }
        }
        FileSystemExchange exchange = new FileSystemExchange(baseDirectory, exchangeStorage, context, outputPartitionCount, secretKey);
        exchange.initialize();
        return exchange;
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle)
    {
        FileSystemExchangeSinkInstanceHandle instanceHandle = (FileSystemExchangeSinkInstanceHandle) handle;
        return new FileSystemExchangeSink(
                exchangeStorage,
                instanceHandle.getOutputDirectory(),
                instanceHandle.getOutputPartitionCount(),
                instanceHandle.getSinkHandle().getSecretKey());
    }

    @Override
    public ExchangeSource createSource(List<ExchangeSourceHandle> handles)
    {
        List<URI> files = handles.stream()
                .map(FileSystemExchangeSourceHandle.class::cast)
                .flatMap(handle -> handle.getFiles().stream())
                .collect(toImmutableList());
        List<Optional<SecretKey>> secretKeys = handles.stream()
                .map(FileSystemExchangeSourceHandle.class::cast)
                .flatMap(handle -> Collections.nCopies(handle.getFiles().size(), handle.getSecretKey()).stream())
                .collect(toImmutableList());
        return new FileSystemExchangeSource(exchangeStorage, files, secretKeys);
    }
}
