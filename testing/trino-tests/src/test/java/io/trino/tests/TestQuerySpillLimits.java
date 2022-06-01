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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spiller.NodeSpillConfig;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingSession.testSessionBuilder;

@Test(singleThreaded = true)
public class TestQuerySpillLimits
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("sf1")
            .build();

    private File spillPath;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        this.spillPath = Files.createTempDirectory(null).toFile();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(spillPath.toPath(), ALLOW_INSECURE);
    }

    @Test(timeOut = 240_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded local spill limit of 10B")
    public void testMaxSpillPerNodeLimit()
    {
        try (QueryRunner queryRunner = createLocalQueryRunner(new NodeSpillConfig().setMaxSpillPerNode(DataSize.succinctBytes(10)))) {
            queryRunner.execute(queryRunner.getDefaultSession(), "SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
        }
    }

    @Test(timeOut = 240_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded per-query local spill limit of 10B")
    public void testQueryMaxSpillPerNodeLimit()
    {
        try (QueryRunner queryRunner = createLocalQueryRunner(new NodeSpillConfig().setQueryMaxSpillPerNode(DataSize.succinctBytes(10)))) {
            queryRunner.execute(queryRunner.getDefaultSession(), "SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
        }
    }

    private LocalQueryRunner createLocalQueryRunner(NodeSpillConfig nodeSpillConfig)
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(SESSION)
                .withFeaturesConfig(
                        new FeaturesConfig()
                                .setSpillerSpillPaths(spillPath.getAbsolutePath())
                                .setSpillEnabled(true))
                .withNodeSpillConfig(nodeSpillConfig)
                .withAlwaysRevokeMemory()
                .build();

        queryRunner.createCatalog(
                SESSION.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        return queryRunner;
    }
}
