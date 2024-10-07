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
package io.trino.spi.connector;

import com.google.errorprone.annotations.CheckReturnValue;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

public interface ConnectorFactory
{
    String getName();

    @CheckReturnValue
    Connector create(String catalogName, Map<String, String> config, ConnectorContext context);

    /**
     * Returns the names of configuration properties for the connector this factory creates
     * that may contain security-sensitive values. The engine will use these names to mask
     * the corresponding property values to prevent the leakage of security-sensitive information.
     */
    default Set<String> getSecuritySensitivePropertyNames()
    {
        return emptySet();
    }
}
