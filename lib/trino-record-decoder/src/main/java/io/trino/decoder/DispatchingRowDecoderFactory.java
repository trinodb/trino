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
package io.trino.decoder;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class DispatchingRowDecoderFactory
{
    private final Map<String, RowDecoderFactory> factories;

    @Inject
    public DispatchingRowDecoderFactory(Map<String, RowDecoderFactory> factories)
    {
        this.factories = ImmutableMap.copyOf(factories);
    }

    public RowDecoder create(ConnectorSession session, RowDecoderSpec rowDecoderSpec)
    {
        checkArgument(factories.containsKey(rowDecoderSpec.dataFormat()), "unknown data format '%s'", rowDecoderSpec.dataFormat());
        return factories.get(rowDecoderSpec.dataFormat()).create(session, rowDecoderSpec);
    }
}
