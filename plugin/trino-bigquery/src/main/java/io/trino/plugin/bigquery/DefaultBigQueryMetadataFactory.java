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
package io.trino.plugin.bigquery;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class DefaultBigQueryMetadataFactory
        implements BigQueryMetadataFactory
{
    private final BigQueryClientFactory bigQueryClient;
    private final BigQueryWriteClientFactory writeClientFactory;
    private final ListeningExecutorService executorService;
    private final BigQueryTypeManager typeManager;
    private final boolean isLegacyMetadataListing;

    @Inject
    public DefaultBigQueryMetadataFactory(
            BigQueryClientFactory bigQueryClient,
            BigQueryWriteClientFactory writeClientFactory,
            BigQueryTypeManager typeManager,
            ListeningExecutorService executorService,
            BigQueryConfig config)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
        this.writeClientFactory = requireNonNull(writeClientFactory, "writeClientFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.isLegacyMetadataListing = config.isLegacyMetadataListing();
    }

    @Override
    public BigQueryMetadata create(BigQueryTransactionHandle transaction)
    {
        return new BigQueryMetadata(bigQueryClient, writeClientFactory, typeManager, executorService, isLegacyMetadataListing);
    }
}
