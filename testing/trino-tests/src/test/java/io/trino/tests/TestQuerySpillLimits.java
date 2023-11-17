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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.nio.file.Files;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestQuerySpillLimits
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("sf1")
            .build();

    private File spillPath;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        this.spillPath = Files.createTempDirectory(null).toFile();
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        deleteRecursively(spillPath.toPath(), ALLOW_INSECURE);
    }

    @Test
    @Timeout(240)
    public void testMaxSpillPerNodeLimit()
    {
        assertThatThrownBy(() -> {
            try (QueryRunner queryRunner = createLocalQueryRunner(new NodeSpillConfig().setMaxSpillPerNode(DataSize.succinctBytes(10)))) {
                queryRunner.execute(queryRunner.getDefaultSession(), "SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
            }
        })
                .isInstanceOf(RuntimeException.class)
                .hasMessage(".*Query exceeded local spill limit of 10B");
    }

    @Test
    @Timeout(240)
    public void testQueryMaxSpillPerNodeLimit()
    {
        assertThatThrownBy(() -> {
            try (QueryRunner queryRunner = createLocalQueryRunner(new NodeSpillConfig().setQueryMaxSpillPerNode(DataSize.succinctBytes(10)))) {
                queryRunner.execute(queryRunner.getDefaultSession(), "SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
            }
        })
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching(".*Query exceeded per-query local spill limit of 10B");
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
