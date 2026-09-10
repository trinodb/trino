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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestInstanceFunction
{
    @Test
    public void test()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertions.addFunctions(InternalFunctionBundle.builder()
                    .scalar(PrecomputedFunction.class)
                    .build());

            assertThat(assertions.expression("precomputed()"))
                    .isEqualTo(42L);
        }
    }

    @Test
    public void testProvidedInstanceFunction()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertions.addFunctions(InternalFunctionBundle.builder()
                    .functions(new StatefulFunction("test:"))
                    .build());

            assertThat(assertions.function("stateful_prefix", "'value'"))
                    .isEqualTo("test:value");
        }
    }

    @ScalarFunction("precomputed")
    public static final class PrecomputedFunction
    {
        private final int value = 42;

        @SqlType(StandardTypes.BIGINT)
        public long precomputed()
        {
            return value;
        }
    }

    public static final class StatefulFunction
    {
        private final String prefix;

        private StatefulFunction(String prefix)
        {
            this.prefix = prefix;
        }

        @ScalarFunction("stateful_prefix")
        @SqlType(StandardTypes.VARCHAR)
        public Slice prefix(@SqlType(StandardTypes.VARCHAR) Slice value)
        {
            return utf8Slice(prefix + value.toStringUtf8());
        }
    }
}
