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
package io.prestosql.plugin.influx;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import io.prestosql.Session;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.prestosql.spi.security.SelectedRole.Type.ROLE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class InfluxQueryTest
{
    private HttpServer httpServer;
    private DistributedQueryRunner queryRunner;
    private Session session;
    private Map<String, String> cannedHttp;
    private final AtomicReference<String> lastQuery = new AtomicReference<>();
    private static ZonedDateTime t1 = getTimestamp("2019-12-10T21:00:04.446Z");
    private static ZonedDateTime t2 = getTimestamp("2019-12-10T21:00:20.446Z");
    private static ZonedDateTime t3 = getTimestamp("2019-12-10T22:00:04.446Z");

    @BeforeClass
    public void beforeClass()
            throws Exception
    {
        // load canned HTTP responses
        cannedHttp = new HashMap<>();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("canned_http.json")) {
            assertNotNull(in);
            new ObjectMapper()
                    .readTree(in)
                    .get("queries")
                    .fields()
                    .forEachRemaining(node -> cannedHttp.put(node.getKey(), node.getValue().toString()));
        }

        // create a real local http server to test against
        Pattern queryExtractor = Pattern.compile("q=([^&]+)");
        httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        httpServer.createContext("/query", exchange -> {
            try {
                String params = exchange.getRequestURI().getQuery();
                assertNotNull(params);
                Matcher queryMatcher = queryExtractor.matcher(params);
                assertTrue(queryMatcher.find());
                String queryString = queryMatcher.group(1);
                queryString = URLDecoder.decode(queryString, StandardCharsets.UTF_8.name());
                lastQuery.set(queryString);
                assertFalse(queryMatcher.find());
                String results = cannedHttp.get(queryString);
                assertNotNull(results, queryString);
                byte[] response = results.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
                exchange.getResponseBody().write(response);
            }
            catch (Throwable t) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
                exchange.getResponseBody().write(new byte[] {});
            }
            finally {
                exchange.close();
            }
        });
        httpServer.start();

        // create a query runner with a real influx connector
        queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                .setIdentity(Identity.forUser("test")
                        .withRole("catalog", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build()).build();
        queryRunner.installPlugin(new InfluxPlugin());
        queryRunner.createCatalog("catalog", "influx",
                new ImmutableMap.Builder<String, String>()
                        .put("influx.port", Integer.toString(httpServer.getAddress().getPort()))
                        .put("influx.database", "test")
                        .build());
        session = queryRunner.getDefaultSession();
    }

    @Test
    public synchronized void testShow()
    {
        MaterializedResult result = queryRunner.getClient().execute(session, "SHOW TABLES").getResult();
        assertEquals(result.getOnlyColumn().collect(Collectors.toList()), Collections.singletonList("data"));

        result = queryRunner.getClient().execute(session, "DESC data")
                .getResult()
                .toTestTypes();
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("time", "timestamp with time zone", "time", "")
                .row("k1", "varchar", "tag", "")
                .row("k2", "varchar", "tag", "")
                .row("v1", "double", "field", "")
                .row("v2", "double", "field", "")
                .build();
        assertEquals(result, expectedColumns);
    }

    @Test
    public synchronized void testSelectAll()
    {
        MaterializedResult result = queryRunner.getClient().execute(session, "SELECT * FROM datA")
                .getResult()
                .toTestTypes();
        assertEquals(lastQuery.get(), "SELECT * FROM SCHEMA.Data");
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, VARCHAR, DOUBLE, DOUBLE)
                .row(t1, "a", "b", 1., 2.)
                .row(t2, "a", "c", 3., 4.)
                .row(t3, "d", "b", 5., 6.)
                .build();
        assertEquals(result, expectedColumns);
    }

    @Test
    public synchronized void testSelectTagEqual()
    {
        MaterializedResult result = queryRunner.getClient().execute(session, "SELECT * FROM data WHERE k1 = 'a'")
                .getResult()
                .toTestTypes();
        assertEquals(lastQuery.get(), "SELECT * FROM SCHEMA.Data WHERE ((k1 = 'a'))");
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, VARCHAR, DOUBLE, DOUBLE)
                .row(t1, "a", "b", 1., 2.)
                .row(t2, "a", "c", 3., 4.)
                .build();
        assertEquals(result, expectedColumns);
    }

    @Test
    public synchronized void testSelectTagNotEqual()
    {
        MaterializedResult result = queryRunner.getClient().execute(session, "SELECT * FROM data WHERE k1 != 'd'")
                .getResult()
                .toTestTypes();
        assertEquals(lastQuery.get(), "SELECT * FROM SCHEMA.Data");  // can't be pushed down
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, VARCHAR, DOUBLE, DOUBLE)
                .row(t1, "a", "b", 1., 2.)
                .row(t2, "a", "c", 3., 4.)
                .build();
        assertEquals(result, expectedColumns);
    }

    @Test
    public synchronized void testSelectTagNull()
    {
        MaterializedResult result = queryRunner.getClient().execute(session, "SELECT * FROM data WHERE k1 IS NULL")
                .getResult()
                .toTestTypes();
        assertEquals(lastQuery.get(), "SELECT * FROM SCHEMA.Data WHERE (k1 = '')");
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, VARCHAR, DOUBLE, DOUBLE)
                .build();
        assertEquals(result, expectedColumns);
    }

    @Test
    public synchronized void testSelectFieldNull()
    {
        MaterializedResult result = queryRunner.getClient().execute(session, "SELECT * FROM data WHERE v1 IS NULL")
                .getResult()
                .toTestTypes();
        assertEquals(lastQuery.get(), "SELECT * FROM SCHEMA.Data");  // can't be pushed down
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, VARCHAR, DOUBLE, DOUBLE)
                .build();
        assertEquals(result, expectedColumns);
    }

    @Test
    public synchronized void testSelectFieldEqual()
    {
        MaterializedResult result = queryRunner.getClient().execute(session, "SELECT * FROM data WHERE v1 = 1")
                .getResult()
                .toTestTypes();
        assertEquals(lastQuery.get(), "SELECT * FROM SCHEMA.Data WHERE ((v1 = 1.0))");
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, VARCHAR, DOUBLE, DOUBLE)
                .row(t1, "a", "b", 1., 2.)
                .build();
        assertEquals(result, expectedColumns);
    }

    @Test
    public synchronized void testSelectFieldBetween()
    {
        MaterializedResult result = queryRunner.getClient().execute(session, "SELECT * FROM data WHERE v1 BETWEEN 1 AND 3")
                .getResult()
                .toTestTypes();
        assertEquals(lastQuery.get(), "SELECT * FROM SCHEMA.Data WHERE ((v1 >= 1.0 AND v1 <= 3.0))");
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, VARCHAR, DOUBLE, DOUBLE)
                .row(t1, "a", "b", 1., 2.)
                .row(t2, "a", "c", 3., 4.)
                .build();
        assertEquals(result, expectedColumns);
    }

    private static ZonedDateTime getTimestamp(String timestamp)
    {
        return ZonedDateTime.ofInstant(Instant.parse(timestamp), ZoneId.of("UTC"));
    }

    @AfterClass
    public void afterClass()
    {
        queryRunner.close();
        httpServer.stop(0);
    }
}
