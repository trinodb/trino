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
import io.trino.spi.Experimental;

import java.util.Map;
import java.util.Set;

public interface ConnectorFactory
{
    String getName();

    @CheckReturnValue
    Connector create(String catalogName, Map<String, String> config, ConnectorContext context);

    /**
     * Returns property names that may contain security-sensitive information.
     * In addition to properties that are explicitly known to the connector as
     * security-sensitive, it may also return properties that are unknown or unused.
     * In other words, if the connector cannot classify a property, it should treat it as
     * security-sensitive by default for safety.
     * <p>
     * The engine uses the properties returned by this method to mask the corresponding
     * values, preventing the leakage of security-sensitive information.
     */
    @Experimental(eta = "2025-12-31")
    default Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return Set.copyOf(config.keySet());
    }
}
