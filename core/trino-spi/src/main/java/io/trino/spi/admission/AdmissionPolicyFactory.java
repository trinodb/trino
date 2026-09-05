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
package io.trino.spi.admission;

import java.util.Map;

/**
 * Factory for {@link AdmissionPolicy}. Discovered via
 * {@code io.trino.spi.Plugin.getAdmissionPolicyFactories()} and selected at
 * coordinator startup by matching {@link #getName()} (case-sensitive) against
 * the configured {@code query-manager.admission-policy.name}.
 */
public interface AdmissionPolicyFactory
{
    /**
     * Stable name used by operators in
     * {@code etc/config.properties:query-manager.admission-policy.name}.
     * MUST be non-null, non-empty, and stable across releases of the plugin.
     */
    String getName();

    /**
     * Build an {@link AdmissionPolicy} from the operator-supplied properties map
     * (parsed from {@code etc/admission-policy.properties}; an empty map is passed
     * when the file is absent).
     *
     * <p>Returning {@code null} or throwing any {@link Throwable} is treated as a
     * coordinator startup failure; the coordinator surfaces an error naming this
     * factory and the underlying cause.
     *
     * @param config non-null property map, possibly empty
     * @return non-null policy instance
     */
    AdmissionPolicy create(Map<String, String> config);
}
