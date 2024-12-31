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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.PART_SUPPLIER;
import static io.trino.tpch.TpchTable.REGION;
import static io.trino.tpch.TpchTable.SUPPLIER;

/**
 * The query runner of trino.
 */
public class PaimonQueryRunner
{
    private static final Logger LOG = Logger.get(PaimonQueryRunner.class);

    private static final String PAIMON_CATALOG = "paimon";

    private PaimonQueryRunner() {}

    public static DistributedQueryRunner createPrestoQueryRunner(
            Map<String, String> extraProperties)
            throws Exception
    {
        return createPrestoQueryRunner(extraProperties, ImmutableMap.of(), true);
    }

    public static DistributedQueryRunner createPrestoQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            boolean createTpchTables)
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(PAIMON_CATALOG).setSchema("tpch").build();

        DistributedQueryRunner queryRunner =
                DistributedQueryRunner.builder(session).setExtraProperties(extraProperties).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("paimon_data");
        Path catalogDir = dataDir.getParent().resolve("catalog");

        queryRunner.installPlugin(new PaimonPlugin());

        Map<String, String> options =
                ImmutableMap.<String, String>builder()
                        .put("warehouse", catalogDir.toFile().toURI().toString())
                        .put("fs.hadoop.enabled", "true")
                        .putAll(extraConnectorProperties)
                        .build();

        queryRunner.createCatalog(PAIMON_CATALOG, PAIMON_CATALOG, options);

        queryRunner.execute("CREATE SCHEMA tpch");

        if (createTpchTables) {
            copyTpchTables(
                    queryRunner,
                    "tpch",
                    TINY_SCHEMA_NAME,
                    session,
                    ImmutableList.of(
                            CUSTOMER,
                            ORDERS,
                            LINE_ITEM,
                            PART,
                            PART_SUPPLIER,
                            SUPPLIER,
                            NATION,
                            REGION));
        }

        return queryRunner;
    }
}
