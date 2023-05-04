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
 */package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorOutputMetadata;

import static java.util.Objects.requireNonNull;

public class DeltaLakeOutputMetadata
    implements ConnectorOutputMetadata
{
    private final DeltaLakeOutputInfo outputInfo;

    @JsonCreator
    public DeltaLakeOutputMetadata(@JsonProperty("outputInfo") DeltaLakeOutputInfo outputInfo)
    {
        this.outputInfo = requireNonNull(outputInfo, "outputInfo is null");
    }

    @Override
    @JsonProperty
    public DeltaLakeOutputInfo getInfo()
    {
        return outputInfo;
    }
}
