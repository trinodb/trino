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
package io.trino.plugin.deltalake.transactionlog.writer;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.spi.TrinoException;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TransactionLogSynchronizerManager
{
    private final Map<String, TransactionLogSynchronizer> synchronizers;
    private final NoIsolationSynchronizer noIsolationSynchronizer;

    @Inject
    public TransactionLogSynchronizerManager(
            Map<String, TransactionLogSynchronizer> synchronizers,
            NoIsolationSynchronizer noIsolationSynchronizer)
    {
        this.synchronizers = ImmutableMap.copyOf(synchronizers);
        this.noIsolationSynchronizer = requireNonNull(noIsolationSynchronizer, "noIsolationSynchronizer is null");
    }

    public TransactionLogSynchronizer getSynchronizer(String tableLocation)
    {
        String uriScheme = Location.of(tableLocation).scheme()
                .orElseThrow(() -> new IllegalArgumentException("URI scheme undefined for " + tableLocation));
        TransactionLogSynchronizer synchronizer = synchronizers.get(uriScheme.toLowerCase(ENGLISH));
        if (synchronizer == null) {
            throw new TrinoException(NOT_SUPPORTED, format("Cannot write to table in %s; %s not supported", tableLocation, uriScheme));
        }
        return synchronizer;
    }

    public TransactionLogSynchronizer getNoIsolationSynchronizer()
    {
        return noIsolationSynchronizer;
    }
}
