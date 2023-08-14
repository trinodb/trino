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
package io.trino.plugin.atop;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.plugin.atop.LocalAtopQueryRunner.createQueryRunner;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestAtopSecurity
{
    private QueryRunner queryRunner;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        Path atopExecutable = Files.createTempFile(null, null);
        String path = new File(Resources.getResource(getClass(), "security.json").toURI()).getPath();
        queryRunner = createQueryRunner(ImmutableMap.of("atop.security", "file", "security.config-file", path, "atop.executable-path", atopExecutable.toString()), TestingAtopFactory.class);
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testAdminCanRead()
    {
        Session admin = getSession("admin");
        queryRunner.execute(admin, "SELECT * FROM disks");
    }

    @Test
    public void testNonAdminCannotRead()
    {
        Session bob = getSession("bob");
        assertThatThrownBy(() -> queryRunner.execute(bob, "SELECT * FROM disks"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching("Access Denied:.*");
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
