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
package io.prestosql.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Set;

import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestQuerySerializationFailures
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner
                .builder(testSessionBuilder().build())
                .setNodeCount(4)
                .build();

        queryRunner.installPlugin(new BogusPlugin());

        return queryRunner;
    }

    @Test
    public void shouldFailOnFirstSerializationError()
    {
        // BOGUS(value) returns BogusType that fails to serialize when value is true
        assertQueryFails("SELECT * FROM (VALUES BOGUS(true), BOGUS(false), BOGUS(true))", "Could not serialize column '_col0' of type 'Bogus' at position 1:1");
    }

    @Test
    public void shouldPass()
    {
        assertQuerySucceeds("SELECT * FROM (VALUES BOGUS(false))");
    }

    public static class BogusPlugin
            implements Plugin
    {
        @Override
        public Iterable<Type> getTypes()
        {
            return ImmutableList.of(BogusType.BOGUS);
        }

        @Override
        public Set<Class<?>> getFunctions()
        {
            return ImmutableSet.of(BogusFunctions.class);
        }
    }

    public static class BogusFunctions
    {
        @Description("Converts value to Bogus type")
        @ScalarFunction("BOGUS")
        @SqlType(BogusType.NAME)
        public static long createBogusValue(@SqlType(BOOLEAN) boolean input)
        {
            return input ? 1 : 0;
        }
    }
}
