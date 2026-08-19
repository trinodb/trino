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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public record ElasticsearchColumnHandle(
        List<String> path,
        Type type,
        IndexMetadata.Type elasticsearchType,
        DecoderDescriptor decoderDescriptor,
        boolean supportsPredicates)
        implements ColumnHandle
{
    public ElasticsearchColumnHandle
    {
        path = ImmutableList.copyOf(path);
        requireNonNull(type, "type is null");
        requireNonNull(elasticsearchType, "elasticsearchType is null");
        requireNonNull(decoderDescriptor, "decoderDescriptor is null");
    }

    /**
     * Trino-facing identifier. Trino metadata exposes identifiers case-insensitively, so this is the normalized lookup
     * name used to resolve SQL such as {@code WHERE ho_ten = ...}.
     */
    @JsonIgnore
    public String logicalName()
    {
        return remoteName().toLowerCase(ENGLISH);
    }

    /**
     * Exact case-sensitive Elasticsearch field path discovered from the index mapping.
     */
    @JsonIgnore
    public String remoteName()
    {
        return Joiner.on('.').join(path);
    }

    /**
     * Backwards-compatible alias for the remote field name. Existing scan/decoder code relies on this method referring
     * to the physical Elasticsearch field, not the Trino-normalized identifier.
     */
    @JsonIgnore
    public String name()
    {
        return remoteName();
    }

    /**
     * Field name to use when pushing predicates into Elasticsearch. For a {@code text} field that has an
     * exact-match {@code keyword} sub-field, this targets the sub-field; otherwise it is the exact remote field name.
     */
    @JsonIgnore
    public String predicateName()
    {
        if (elasticsearchType instanceof IndexMetadata.PrimitiveType primitiveType && primitiveType.keyword().isPresent()) {
            return remoteName() + "." + primitiveType.keyword().get();
        }
        return remoteName();
    }

    @Override
    public String toString()
    {
        return remoteName() + "::" + type();
    }
}
