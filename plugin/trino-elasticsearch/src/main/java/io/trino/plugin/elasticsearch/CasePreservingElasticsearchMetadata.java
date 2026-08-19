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
package io.trino.plugin.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.type.TypeManager;

import java.util.Map;

/**
 * Exposes Trino-normalized column identifiers while preserving the original Elasticsearch field path in the
 * {@link ElasticsearchColumnHandle}. Trino clients can therefore use the lowercase column names returned by metadata,
 * while every Elasticsearch request continues to target the case-sensitive remote field name stored in the handle.
 */
public class CasePreservingElasticsearchMetadata
        extends ElasticsearchMetadata
{
    @Inject
    public CasePreservingElasticsearchMetadata(TypeManager typeManager, ElasticsearchClient client, ElasticsearchConfig config)
    {
        super(typeManager, client, config);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return normalizeColumnHandles(super.getColumnHandles(session, tableHandle));
    }

    static Map<String, ColumnHandle> normalizeColumnHandles(Map<String, ColumnHandle> handles)
    {
        ImmutableMap.Builder<String, ColumnHandle> normalized = ImmutableMap.builder();
        handles.values().stream()
                .map(ElasticsearchColumnHandle.class::cast)
                .forEach(handle -> normalized.put(handle.logicalName(), handle));
        return normalized.buildOrThrow();
    }
}
