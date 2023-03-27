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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.spanner.SpannerPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.SystemAccessControl;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class SpannerSqlQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";
    //url for emulated spanner host
    private static final String JDBC_URL = "jdbc:cloudspanner://0.0.0.0:9010/projects/spanner-project/instances/spanner-instance/databases/spanner-database;autoConfigEmulator=true";
    private static final String USER = "";
    private static final String PASSWORD = "";
    private static final String SCHEMA = "";
    private static Optional<String> warehouse;
    private static Optional<String> catalog;

    private SpannerSqlQueryRunner()
    {
    }

    private static String getResourcePath(String resourceName)
    {
        return requireNonNull(SpannerSqlQueryRunner.class.getClassLoader().getResource(resourceName), "Resource does not exist: " + resourceName).getPath();
    }

    private static SystemAccessControl newFileBasedSystemAccessControl(String resourceName)
    {
        return newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", getResourcePath(resourceName)));
    }

    private static SystemAccessControl newFileBasedSystemAccessControl(ImmutableMap<String, String> config)
    {
        return new FileBasedSystemAccessControl.Factory().create(config);
    }

    public static DistributedQueryRunner createSpannerSqlQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Consumer<QueryRunner> moreSetup)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .setCoordinatorProperties(coordinatorProperties)
                    .setAdditionalSetup(moreSetup);
            queryRunner = builder.build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", JDBC_URL);
            connectorProperties.putIfAbsent("connection-user", USER);
            connectorProperties.putIfAbsent("connection-password", PASSWORD);
            connectorProperties.putIfAbsent("spanner.credentials.file", "credentials.json");
            queryRunner.installPlugin(new SpannerPlugin());
            queryRunner.createCatalog("spanner",
                    "spanner", connectorProperties);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("spanner")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static DistributedQueryRunner createSpannerSqlQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = createSpannerSqlQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"
                        //Property to skip columns from select * which user does not access to
                        //,"hide-inaccessible-columns", "true"
                ),
                ImmutableMap.of(),
                ImmutableMap.of(),
                TpchTable.getTables(),
                r -> {}
        );

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");
        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = getQueryRunner();

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");
        MaterializedResult schemas = queryRunner.execute("SHOW SCHEMAS FROM spanner");
        System.out.println(schemas);
        MaterializedResult execute = queryRunner.execute("SHOW TABLES FROM spanner.default");
        System.out.println(execute);
        MaterializedResult emp = queryRunner.execute("create table if not exists spanner.default.emp" +
                "( id int,name varchar" +
                ") WITH(primary_keys = ARRAY['id'])");
        System.out.println(emp);
        MaterializedResult create = queryRunner.execute("create table if not exists spanner.default.dept" +
                "( id int,name varchar" +
                ")WITH (primary_keys = ARRAY['id']," +
                "interleave_in_parent='emp'," +
                "on_delete_cascade=true" +
                ")");
        System.out.println(create);
        MaterializedResult insert = queryRunner.execute("insert into spanner.default.emp values(1,'Tom')");
        System.out.println(insert);
        MaterializedResult count = queryRunner.execute("select count(*) from spanner.default.emp");
        System.out.println(count);

        MaterializedResult drop = queryRunner.execute("DROP TABLE spanner.default.dept");
        System.out.println(drop);

        System.out.println("DONE");

        Logger log = Logger.get(SpannerSqlQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    static DistributedQueryRunner getQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = createSpannerSqlQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                ImmutableMap.of(),
                TpchTable.getTables()
                , xr -> {});
        return queryRunner;
    }


}
