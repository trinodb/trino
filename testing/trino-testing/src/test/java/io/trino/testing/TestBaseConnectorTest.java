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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBaseConnectorTest
{
    @Test
    public void testThatAllTestsAreCopied()
    {
        assertThat(getMethods(BaseConnectorTest.class)).containsAll(getMethods(AbstractTestIntegrationSmokeTest.class));
    }

    private List<MethodSignature> getMethods(Class<?> clazz)
    {
        return Arrays.stream(clazz.getMethods())
                .map(method -> new MethodSignature(method.getName(), method.getParameterTypes()))
                .collect(toImmutableList());
    }

    private static final class MethodSignature
    {
        private final String name;
        private final List<Class<?>> parameters;

        private MethodSignature(String name, Class<?>[] parameters)
        {
            this.name = requireNonNull(name, "name is null");
            this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MethodSignature that = (MethodSignature) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(parameters, that.parameters);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, parameters);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("fields", parameters)
                    .toString();
        }
    }
}
