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
package io.trino.tests.product.launcher.env.common;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.Environment;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;

public interface EnvironmentExtender
{
    void extendEnvironment(Environment.Builder builder);

    default Optional<String> getExtraOptionsPrefix()
    {
        return Optional.empty();
    }

    default void setExtraOptions(Map<String, String> extraOptions)
    {
        verify(getExtraOptionsPrefix().isEmpty(), "getExtraOptionsPrefix is defined but setExtraOptions not overridden");
        throw new UnsupportedOperationException("Implementations must override this to consume extra options");
    }

    /**
     * First dependencies extend the environment, then this {@link EnvironmentExtender} is used.
     */
    default List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of();
    }
}
