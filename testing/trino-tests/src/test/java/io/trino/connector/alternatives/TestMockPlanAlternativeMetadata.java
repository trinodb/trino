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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.hudi.HudiMetadata;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestMockPlanAlternativeMetadata
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorMetadata.class, MockPlanAlternativeMetadata.class, ImmutableSet.<Method>builder()
                // Do not require implementation of deprecated methods, as they are not called by the engine
                .addAll(Stream.of(ConnectorMetadata.class.getMethods())
                        .filter(method -> method.isAnnotationPresent(Deprecated.class))
                        .collect(toImmutableList()))
                // Require MockPlanAlternativeMetadata to implement at least all methods implemented by data lakes connectors
                .addAll(Stream.of(ConnectorMetadata.class.getMethods())
                        .filter(method -> !overrides(HiveMetadata.class, method) &&
                                !overrides(IcebergMetadata.class, method) &&
                                !overrides(DeltaLakeMetadata.class, method) &&
                                !overrides(HudiMetadata.class, method))
                        // ... but also allow it to implement more methods (probably not needed by tests)
                        .filter(method -> !overrides(MockPlanAlternativeMetadata.class, method))
                        .collect(toImmutableList()))
                .build());
    }

    private static boolean overrides(Class<?> implementation, Method interfaceMethod)
    {
        Class<?> interfaceClass = interfaceMethod.getDeclaringClass();
        checkArgument(interfaceClass.isInterface());
        checkArgument(!implementation.isInterface());
        checkArgument(interfaceClass.isAssignableFrom(implementation));
        try {
            return implementation.getMethod(interfaceMethod.getName(), interfaceMethod.getParameterTypes()).getDeclaringClass() != interfaceClass;
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
