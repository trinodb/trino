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

package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.prestosql.spi.connector.SchemaTableName;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.prestosql.spi.testing.InterfaceTestUtils.assertProperForwardingMethodsAreCalled;

public class TestCachingJdbcClient
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(JdbcClient.class, CachingJdbcClient.class);
    }

    @Test
    public void testProperForwardingMethodsAreCalled()
    {
        assertProperForwardingMethodsAreCalled(
                JdbcClient.class,
                jdbcClient -> new CachingJdbcClient(jdbcClient, new BaseJdbcConfig().setMetadataCacheTtl(Duration.valueOf("0s"))),
                clazz -> {
                    if (clazz.equals(JdbcIdentity.class)) {
                        return new JdbcIdentity("user", ImmutableMap.of());
                    }
                    if (clazz.equals(JdbcSplit.class)) {
                        return new JdbcSplit(Optional.empty());
                    }
                    if (clazz.equals(JdbcTableHandle.class)) {
                        return new JdbcTableHandle(new SchemaTableName("schema", "table"), null, null, "table");
                    }
                    return null;
                });
    }
}
