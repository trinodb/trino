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
package io.trino.connector.alternatives;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class MockPlanAlternativePlugin
        implements Plugin
{
    private final List<Plugin> delegates;

    public MockPlanAlternativePlugin(Plugin... delegates)
    {
        this.delegates = ImmutableList.copyOf(delegates);
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return delegates.stream()
                .flatMap(delegate -> StreamSupport.stream(delegate.getConnectorFactories().spliterator(), false))
                .map(MockPlanAlternativeConnectorFactory::new)
                .collect(toImmutableList());
    }
}
