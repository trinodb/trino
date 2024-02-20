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
package io.trino.hdfs.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.security.ConnectorIdentity;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class S3SecurityMappings
{
    private final List<S3SecurityMapping> mappings;

    @JsonCreator
    public S3SecurityMappings(@JsonProperty("mappings") List<S3SecurityMapping> mappings)
    {
        this.mappings = ImmutableList.copyOf(requireNonNull(mappings, "mappings is null"));
    }

    public Optional<S3SecurityMapping> getMapping(ConnectorIdentity identity, URI uri)
    {
        return mappings.stream()
                .filter(mapping -> mapping.matches(identity, uri))
                .findFirst();
    }
}
