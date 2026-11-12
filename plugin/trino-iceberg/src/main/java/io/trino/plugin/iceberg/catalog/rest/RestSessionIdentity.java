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
package io.trino.plugin.iceberg.catalog.rest;

import io.trino.plugin.base.metrics.ApiCallStats;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

/**
 * Carried as {@code SessionContext#wrappedIdentity} so that the FileIO created by the REST session catalog
 * can record file system calls into the stats of the {@link TrinoRestCatalog} that issued the request.
 */
record RestSessionIdentity(ConnectorIdentity identity, ApiCallStats fileSystemStats)
{
    RestSessionIdentity
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(fileSystemStats, "fileSystemStats is null");
    }
}
