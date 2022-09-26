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
package io.trino.execution;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.Plugin;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.tree.DropJar;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestDropJarTask
{
    private final String jarPath = "/Users/xx/presto-udfs/target/";

    protected LocalQueryRunner queryRunner;

    @BeforeMethod
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
    }

    @AfterMethod
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDropJar()
    {
        DropJarTask task = getDropJarTask();
        DropJar statement = new DropJar(jarPath, true);
        getFutureValue(task.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertFalse(queryRunner.getFunctionJarDynamicManager().getFunctionJarStore().exists(jarPath));
    }

    // just for syntax demo
    private void testDrop()
    {
        queryRunner.execute("drop jar if exists 'aa'");
    }

    private DropJarTask getDropJarTask()
    {
        return new DropJarTask(queryRunner.getFunctionJarDynamicManager());
    }

    private QueryStateMachine createNewQuery()
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                "test",
                Optional.empty(),
                queryRunner.getDefaultSession(),
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                queryRunner.getTransactionManager(),
                queryRunner.getAccessControl(),
                directExecutor(),
                queryRunner.getMetadata(),
                WarningCollector.NOOP,
                Optional.empty());
    }

    public class UdfPlugin
            implements Plugin
    {
        @Override
        public Set<Class<?>> getFunctions()
        {
            /*
             * trino 0.157 does not expose the interfaces to add SqlFunction objects directly
             * We can only add udfs via Annotations now
             *
             * Unsupported udfs right now:
             * Hash
             * Nvl
             */
            return ImmutableSet.<Class<?>>builder()
                    .add(ExtendedStringFunctions.class)
                    .build();
        }
    }

    public static class ExtendedStringFunctions
    {
        private ExtendedStringFunctions() {}

        @Description("returns index of first occurrence of a substring (or 0 if not found) in string")
        @ScalarFunction("instr")
        @SqlType(StandardTypes.BIGINT)
        public static long inString(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice substring)
        {
            if (substring.length() == 0) {
                return 1;
            }
            if (string.length() == 0) {
                return 0;
            }
            return stringPosition(string, substring);
        }

        private static long stringPosition(Slice string, Slice substring)
        {
            if (substring.length() == 0) {
                return 1L;
            }
            else {
                int index = string.indexOf(substring);
                return index < 0 ? 0L : (long) (SliceUtf8.countCodePoints(string, 0, index) + 1);
            }
        }
    }
}
