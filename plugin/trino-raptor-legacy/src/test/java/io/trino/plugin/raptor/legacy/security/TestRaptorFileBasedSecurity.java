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
package io.trino.plugin.raptor.legacy.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;

import static io.trino.plugin.raptor.legacy.RaptorQueryRunner.createRaptorQueryRunner;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRaptorFileBasedSecurity
{
    private final QueryRunner queryRunner;

    public TestRaptorFileBasedSecurity()
            throws Exception
    {
        String path = new File(Resources.getResource(getClass(), "security.json").toURI()).getPath();
        queryRunner = createRaptorQueryRunner(
                ImmutableMap.of(),
                TpchTable.getTables(),
                false,
                ImmutableMap.of("security.config-file", path, "raptor.security", "file"));
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testAdminCanRead()
    {
        Session admin = getSession("user");
        queryRunner.execute(admin, "SELECT * FROM orders");
    }

    @Test
    public void testNonAdminCannotRead()
    {
        assertThatThrownBy(() -> {
            Session bob = getSession("bob");
            queryRunner.execute(bob, "SELECT * FROM orders");
        })
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching(".*Access Denied: Cannot select from table tpch.orders.*");
    }

    private Session getSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(queryRunner.getDefaultSession().getCatalog())
                .setSchema(queryRunner.getDefaultSession().getSchema())
                .setIdentity(Identity.ofUser(user))
                .build();
    }
}
