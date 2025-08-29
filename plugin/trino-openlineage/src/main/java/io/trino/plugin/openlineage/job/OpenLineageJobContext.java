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
package io.trino.plugin.openlineage.job;

import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryMetadata;

import static java.util.Objects.requireNonNull;

public record OpenLineageJobContext(QueryContext queryContext, QueryMetadata queryMetadata)
{
    public OpenLineageJobContext
    {
        requireNonNull(queryContext, "queryContext is null");
        requireNonNull(queryMetadata, "queryMetadata is null");
    }
}
