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
package io.trino.plugin.paimon;

import com.google.inject.Inject;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.util.List;

/**
 * Trino {@link ConnectorNodePartitioningProvider}.
 */
public class PaimonNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    @Inject
    public PaimonNodePartitioningProvider() {}

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int workerCount)
    {
        // todo support dynamic bucket tables
        PaimonPartitioningHandle paimonPartitioningHandle =
                (PaimonPartitioningHandle) partitioningHandle;
        return new FixedBucketTableShuffleFunction(
                partitionChannelTypes, paimonPartitioningHandle, workerCount);
    }
}
