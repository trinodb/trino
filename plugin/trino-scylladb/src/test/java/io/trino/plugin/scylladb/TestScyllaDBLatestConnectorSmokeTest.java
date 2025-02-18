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
<<<<<<<< HEAD:plugin/trino-scylladb/src/test/java/io/trino/plugin/scylladb/TestScyllaDBLatestConnectorSmokeTest.java
package io.trino.plugin.scylladb;

========
package io.trino.plugin.scylla;

import com.google.common.collect.ImmutableMap;
>>>>>>>> scylladb-own-connecotor:plugin/trino-scylla/src/test/java/io/trino/plugin/scylla/TestScyllaConnectorSmokeTest.java
import io.trino.plugin.cassandra.BaseCassandraConnectorSmokeTest;
import io.trino.plugin.cassandra.CassandraSession;
import io.trino.testing.QueryRunner;

import java.sql.Timestamp;

import static io.trino.plugin.cassandra.CassandraTestingUtils.createTestTables;
<<<<<<<< HEAD:plugin/trino-scylladb/src/test/java/io/trino/plugin/scylladb/TestScyllaDBLatestConnectorSmokeTest.java
========
import static io.trino.plugin.scylla.ScyllaQueryRunner.createScyllaQueryRunner;
>>>>>>>> scylladb-own-connecotor:plugin/trino-scylla/src/test/java/io/trino/plugin/scylla/TestScyllaConnectorSmokeTest.java

public class TestScyllaDBLatestConnectorSmokeTest
        extends BaseCassandraConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
<<<<<<<< HEAD:plugin/trino-scylladb/src/test/java/io/trino/plugin/scylladb/TestScyllaDBLatestConnectorSmokeTest.java
        TestingScyllaDBServer server = closeAfterClass(new TestingScyllaDBServer());
========
        TestingScyllaServer server = closeAfterClass(new TestingScyllaServer());
>>>>>>>> scylladb-own-connecotor:plugin/trino-scylla/src/test/java/io/trino/plugin/scylla/TestScyllaConnectorSmokeTest.java
        CassandraSession session = server.getSession();
        createTestTables(session, KEYSPACE, Timestamp.from(TIMESTAMP_VALUE.toInstant()));
        return ScyllaDBQueryRunner.builder(server).setInitialTables(REQUIRED_TPCH_TABLES).build();
    }
}
