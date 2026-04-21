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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorOutputMetadata;

import java.util.Map;

final class IcebergCommitMetadata
        implements ConnectorOutputMetadata
{
    private final Map<String, String> commitMetrics;

    @JsonCreator
    public IcebergCommitMetadata(@JsonProperty("commitMetrics") Map<String, String> commitMetrics)
    {
        this.commitMetrics = ImmutableMap.copyOf(commitMetrics);
    }

    @Override
    @JsonProperty
    public Map<String, String> getInfo()
    {
        return commitMetrics;
    }
}
