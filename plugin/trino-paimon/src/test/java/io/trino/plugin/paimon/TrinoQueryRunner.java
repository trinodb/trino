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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TrinoQueryRunner
{
    private static final Logger LOG = Logger.get(TrinoQueryRunner.class);

    private static final String PAIMON_CATALOG = "paimon";

    private TrinoQueryRunner() {}

    public static DistributedQueryRunner createPrestoQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        return createPrestoQueryRunner(extraProperties, Map.of(), false);
    }

    public static DistributedQueryRunner createPrestoQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            boolean createTpchTables)
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(PAIMON_CATALOG).setSchema("tpch").build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("paimon_data");
        Path catalogDir = dataDir.getParent().resolve("catalog");

        queryRunner.installPlugin(new PaimonPlugin());

        Map<String, String> options = ImmutableMap.<String, String>builder()
                .put("warehouse", catalogDir.toFile().toURI().toString())
                .put("fs.local.enabled", "true")
                .putAll(extraConnectorProperties).buildOrThrow();

        queryRunner.createCatalog(PAIMON_CATALOG, PAIMON_CATALOG, options);

        queryRunner.execute("CREATE SCHEMA tpch");

        if (createTpchTables) {
            copyTpchTablesToPaimon(queryRunner, session);
        }

        return queryRunner;
    }

    /**
     * Copies essential TPCH tables to Paimon catalog for testing. This includes
     * NATION, REGION, and ORDERS tables required by BaseConnectorSmokeTest and
     * AbstractDistributedEngineOnlyQueries.
     *
     * NOTE: Tables are created with HASH_FIXED bucket mode to support DELETE/MERGE
     * operations. BUCKET_UNAWARE mode does not support these operations in Paimon
     * connector.
     */
    private static void copyTpchTablesToPaimon(DistributedQueryRunner queryRunner, Session session)
    {
        // Copy NATION table with HASH_FIXED bucket mode
        // Schema: nationkey (BIGINT PRIMARY KEY), name, regionkey, comment
        queryRunner.execute(session,
                "CREATE TABLE nation (" + "  nationkey BIGINT, " + "  name VARCHAR(25), " + "  regionkey BIGINT, "
                        + "  comment VARCHAR(152)" + ") WITH (" + "  primary_key = ARRAY['nationkey'], "
                        + "  bucket = '1', " + // HASH_FIXED mode with 1 bucket
                        "  bucket_key = 'nationkey'" + // Explicitly specify bucket key to ensure HASH_FIXED
                        ")");

        queryRunner.execute(session, "INSERT INTO nation SELECT * FROM tpch.tiny.nation");

        // Copy REGION table with HASH_FIXED bucket mode
        // Schema: regionkey (BIGINT PRIMARY KEY), name, comment
        queryRunner.execute(session, "CREATE TABLE region (" + "  regionkey BIGINT, " + "  name VARCHAR(25), "
                + "  comment VARCHAR(152)" + ") WITH (" + "  primary_key = ARRAY['regionkey'], " + "  bucket = '1', " + // HASH_FIXED
                // mode
                // with
                // 1
                // bucket
                "  bucket_key = 'regionkey'" + // Explicitly specify bucket key to ensure HASH_FIXED
                ")");

        queryRunner.execute(session, "INSERT INTO region SELECT * FROM tpch.tiny.region");

        // Copy ORDERS table - most commonly used in distributed query tests
        // Schema: orderkey (BIGINT PRIMARY KEY), custkey, orderstatus, totalprice,
        // orderdate, orderpriority,
        // clerk, shippriority, comment
        queryRunner.execute(session,
                "CREATE TABLE orders (" + "  orderkey BIGINT, " + "  custkey BIGINT, " + "  orderstatus VARCHAR(1), "
                        + "  totalprice DOUBLE, " + "  orderdate DATE, " + "  orderpriority VARCHAR(15), "
                        + "  clerk VARCHAR(15), " + "  shippriority INTEGER, " + "  comment VARCHAR(79)" + ") WITH ("
                        + "  primary_key = ARRAY['orderkey'], " + "  bucket = '1', " + "  bucket_key = 'orderkey'"
                        + ")");

        queryRunner.execute(session, "INSERT INTO orders SELECT * FROM tpch.tiny.orders");

        // Copy CUSTOMER table - needed for join tests
        queryRunner.execute(session,
                "CREATE TABLE customer (" + "  custkey BIGINT, " + "  name VARCHAR(25), " + "  address VARCHAR(40), "
                        + "  nationkey BIGINT, " + "  phone VARCHAR(15), " + "  acctbal DOUBLE, "
                        + "  mktsegment VARCHAR(10), " + "  comment VARCHAR(117)" + ") WITH ("
                        + "  primary_key = ARRAY['custkey'], " + "  bucket = '1', " + "  bucket_key = 'custkey'" + ")");

        queryRunner.execute(session, "INSERT INTO customer SELECT * FROM tpch.tiny.customer");

        // Copy LINEITEM table - largest table, many tests use it
        queryRunner.execute(session,
                "CREATE TABLE lineitem (" + "  orderkey BIGINT, " + "  partkey BIGINT, " + "  suppkey BIGINT, "
                        + "  linenumber INTEGER, " + "  quantity DOUBLE, " + "  extendedprice DOUBLE, "
                        + "  discount DOUBLE, " + "  tax DOUBLE, " + "  returnflag VARCHAR(1), "
                        + "  linestatus VARCHAR(1), " + "  shipdate DATE, " + "  commitdate DATE, "
                        + "  receiptdate DATE, " + "  shipinstruct VARCHAR(25), " + "  shipmode VARCHAR(10), "
                        + "  comment VARCHAR(44)" + ") WITH (" + "  primary_key = ARRAY['orderkey', 'linenumber'], "
                        + "  bucket = '1', " + "  bucket_key = 'orderkey,linenumber'" + ")");

        queryRunner.execute(session, "INSERT INTO lineitem SELECT * FROM tpch.tiny.lineitem");

        // Copy PART table - needed for distributed query tests
        queryRunner.execute(session,
                "CREATE TABLE part (" + "  partkey BIGINT, " + "  name VARCHAR(55), " + "  mfgr VARCHAR(25), "
                        + "  brand VARCHAR(10), " + "  type VARCHAR(25), " + "  size INTEGER, "
                        + "  container VARCHAR(10), " + "  retailprice DOUBLE, " + "  comment VARCHAR(23)" + ") WITH ("
                        + "  primary_key = ARRAY['partkey'], " + "  bucket = '1', " + "  bucket_key = 'partkey'" + ")");

        queryRunner.execute(session, "INSERT INTO part SELECT * FROM tpch.tiny.part");

        // Copy SUPPLIER table - needed for distributed query tests
        queryRunner.execute(session,
                "CREATE TABLE supplier (" + "  suppkey BIGINT, " + "  name VARCHAR(25), " + "  address VARCHAR(40), "
                        + "  nationkey BIGINT, " + "  phone VARCHAR(15), " + "  acctbal DOUBLE, "
                        + "  comment VARCHAR(101)" + ") WITH (" + "  primary_key = ARRAY['suppkey'], "
                        + "  bucket = '1', " + "  bucket_key = 'suppkey'" + ")");

        queryRunner.execute(session, "INSERT INTO supplier SELECT * FROM tpch.tiny.supplier");

        // Copy PARTSUPP table - needed for distributed query tests
        queryRunner.execute(session,
                "CREATE TABLE partsupp (" + "  partkey BIGINT, " + "  suppkey BIGINT, " + "  availqty INTEGER, "
                        + "  supplycost DOUBLE, " + "  comment VARCHAR(199)" + ") WITH ("
                        + "  primary_key = ARRAY['partkey', 'suppkey'], " + "  bucket = '1', "
                        + "  bucket_key = 'partkey,suppkey'" + ")");

        queryRunner.execute(session, "INSERT INTO partsupp SELECT * FROM tpch.tiny.partsupp");
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = createPrestoQueryRunner(properties);
        }
        catch (Throwable t) {
            LOG.error(t);
            System.exit(1);
        }
        TimeUnit.MILLISECONDS.sleep(10);
        Logger log = Logger.get(TrinoQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
