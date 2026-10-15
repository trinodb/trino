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
package io.trino.plugin.iceberg.catalog.rest;

import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static java.lang.reflect.Modifier.isStatic;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRestCatalogS3RemoteSigningClient
{
    @Test
    void testHttpClientMethodsOverridden()
            throws Exception
    {
        for (Method method : RESTClient.class.getMethods()) {
            if (isStatic(method.getModifiers())) {
                continue;
            }
            Method delegateMethod = HTTPClient.class.getMethod(method.getName(), method.getParameterTypes());
            if (delegateMethod.getDeclaringClass().isInterface()) {
                continue;
            }

            Method wrapperMethod = IcebergRestCatalogS3RemoteSigningClient.class.getMethod(method.getName(), method.getParameterTypes());
            assertThat(wrapperMethod.getDeclaringClass().isInterface())
                    .as("Missing RESTClient override for %s", method)
                    .isFalse();
        }
    }
}
