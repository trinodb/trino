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
package io.trino.server;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPluginSharedPackages
{
    private static final String SERVER_CLASS = Strings.class.getName();

    @Test
    public void testServerClassIsNotVisibleByDefault()
            throws Exception
    {
        try (PluginClassLoader loader = PluginManager.createClassLoader("test", List.of())) {
            assertThatThrownBy(() -> loader.loadClass(SERVER_CLASS))
                    .isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test
    public void testSharedPackageLoadsFromServer()
            throws Exception
    {
        try (PluginClassLoader loader = PluginManager.createClassLoader("test", List.of(), List.of("com.google.common.base."))) {
            Class<?> loaded = loader.loadClass(SERVER_CLASS);
            assertThat(loaded).isSameAs(Strings.class);
            assertThat(loaded.getClassLoader()).isSameAs(PluginManager.class.getClassLoader());
        }
    }

    @Test
    public void testOtherPackagesStayIsolated()
            throws Exception
    {
        try (PluginClassLoader loader = PluginManager.createClassLoader("test", List.of(), List.of("com.google.common.base."))) {
            assertThatThrownBy(() -> loader.loadClass("com.google.common.collect.ImmutableList"))
                    .isInstanceOf(ClassNotFoundException.class);
        }
    }
}
