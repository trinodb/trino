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
import com.google.inject.Key;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.connector.DynamicCatalogManagerModule;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestTaskInitialization
        extends AbstractTestQueryFramework
{
    Session defaultSession = testSessionBuilder()
            .setCatalog("memory")
            .setSchema("default")
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner
                .builder(defaultSession)
                .addCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .setNodeCount(3)
                .setAdditionalModule(binder -> {
                    OptionalBinder.newOptionalBinder(binder, Key.get(Duration.class, DynamicCatalogManagerModule.ForCatalogBuildDelay.class))
                            .setBinding().toInstance(new Duration(60, TimeUnit.SECONDS));
                })
                .setAdditionalSetup(queryRunner ->
                {
                    queryRunner.installPlugin(new TpchPlugin());
                    queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
                    queryRunner.installPlugin(new TpcdsPlugin());
                    queryRunner.createCatalog("tpcds", "tpcds", ImmutableMap.of());
                })
                .build();
    }

    @Test
    public void testSimpleQueriesWithCatalogCreateDelay()
    {
        query("SELECT * from tpch.tiny.lineitem").assertThat().succeeds();
        query("SELECT * from tpcds.tiny.call_center").assertThat().succeeds();
    }
}
