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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;
import io.trino.spi.connector.SchemaTableName;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public final class CassandraTestingUtils
{
    public static final CassandraTypeManager CASSANDRA_TYPE_MANAGER = new CassandraTypeManager(TESTING_TYPE_MANAGER);

    public static final String TABLE_ALL_TYPES = "table_all_types";
    public static final String TABLE_TUPLE_TYPE = "table_tuple_type";
    public static final String TABLE_USER_DEFINED_TYPE = "table_user_defined_type";
    public static final String TABLE_DELETE_DATA = "table_delete_data";

    private CassandraTestingUtils() {}

    public static void createTestTables(CassandraSession cassandraSession, String keyspace, Date date)
    {
        createKeyspace(cassandraSession, keyspace);
        createTableAllTypes(cassandraSession, new SchemaTableName(keyspace, TABLE_ALL_TYPES), date, 9);
        createTableTupleType(cassandraSession, new SchemaTableName(keyspace, TABLE_TUPLE_TYPE));
        createTableUserDefinedType(cassandraSession, new SchemaTableName(keyspace, TABLE_USER_DEFINED_TYPE));
        createTableDeleteData(cassandraSession, new SchemaTableName(keyspace, TABLE_DELETE_DATA));
    }

    public static void createKeyspace(CassandraSession session, String keyspaceName)
    {
        session.execute("CREATE KEYSPACE " + keyspaceName + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
    }

    public static void createTableAllTypes(CassandraSession session, SchemaTableName table, Date date, int rowsCount)
    {
        session.execute("DROP TABLE IF EXISTS " + table);
        session.execute("CREATE TABLE " + table + " (" +
                " key text PRIMARY KEY, " +
                " typeuuid uuid, " +
                " typetinyint tinyint, " +
                " typesmallint smallint, " +
                " typeinteger int, " +
                " typelong bigint, " +
                " typebytes blob, " +
                " typedate date, " +
                " typetimestamp timestamp, " +
                " typeansi ascii, " +
                " typeboolean boolean, " +
                " typedecimal decimal, " +
                " typedouble double, " +
                " typefloat float, " +
                " typeinet inet, " +
                " typevarchar varchar, " +
                " typevarint varint, " +
                " typetimeuuid timeuuid, " +
                " typelist list<text>, " +
                " typemap map<int, bigint>, " +
                " typeset set<boolean>, " +
                ")");
        insertTestData(session, table, date, rowsCount);
    }

    public static void createTableTupleType(CassandraSession session, SchemaTableName table)
    {
        session.execute("DROP TABLE IF EXISTS " + table);

        session.execute("CREATE TABLE " + table + " (" +
                " key int PRIMARY KEY, " +
                " typetuple frozen<tuple<int, text, float>>" +
                ")");
        session.execute(format("INSERT INTO %s (key, typetuple) VALUES (1, (1, 'text-1', 1.11))", table));
        session.execute(format("INSERT INTO %s (key, typetuple) VALUES (2, (2, 'text-2', 2.22))", table));

        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), 2);
    }

    private static void createTableUserDefinedType(CassandraSession session, SchemaTableName table)
    {
        String typeName = "type_user_defined";
        String nestedTypeName = "type_nested_user_defined";

        session.execute("DROP TABLE IF EXISTS " + table);
        session.execute(format("DROP TYPE IF EXISTS %s.%s", table.getSchemaName(), typeName));

        session.execute(format("CREATE TYPE %s.%s (nestedinteger int)", table.getSchemaName(), nestedTypeName));
        session.execute(format("CREATE TYPE %s.%s (" +
                "typetext text, " +
                "typeuuid uuid, " +
                "typeinteger int, " +
                "typelong bigint, " +
                "typebytes blob, " +
                "typetimestamp timestamp, " +
                "typeansi ascii, " +
                "typeboolean boolean, " +
                "typedecimal decimal, " +
                "typedouble double, " +
                "typefloat float, " +
                "typeinet inet, " +
                "typevarchar varchar, " +
                "typevarint varint, " +
                "typetimeuuid timeuuid, " +
                "typelist frozen <list<text>>, " +
                "typemap frozen <map<varchar, bigint>>, " +
                "typeset frozen <set<boolean>>, " +
                "typetuple frozen <tuple<int>>, " +
                "typenestedudt frozen <%s> " +
                ")", table.getSchemaName(), typeName, nestedTypeName));

        session.execute(format("CREATE TABLE %s (" +
                "key text PRIMARY KEY, " +
                "typeudt frozen <%s>, " +
                ")", table, typeName));

        session.execute(format("INSERT INTO %s (key, typeudt) VALUES (" +
                "'key'," +
                "{ " +
                "typetext: 'text', " +
                "typeuuid: 01234567-0123-0123-0123-0123456789ab, " +
                "typeinteger: -2147483648, " +
                "typelong:  -9223372036854775808," +
                "typebytes: 0x3031323334, " +
                "typetimestamp: '1970-01-01 08:00:00', " +
                "typeansi: 'ansi', " +
                "typeboolean: true, " +
                "typedecimal: 99999999999999997748809823456034029568, " +
                "typedouble: 4.9407e-324, " +
                "typefloat: 1.4013e-45, " +
                "typeinet: '0.0.0.0', " +
                "typevarchar: 'varchar', " +
                "typevarint: -9223372036854775808, " +
                "typetimeuuid: d2177dd0-eaa2-11de-a572-001b779c76e3, " +
                "typelist: ['list'], " +
                "typemap: {'map': 1}, " +
                "typeset: {true}, " +
                "typetuple: (123), " +
                "typenestedudt: {nestedinteger: 999} " +
                "}" +
                ");", table));

        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), 1);
    }

    private static void insertTestData(CassandraSession session, SchemaTableName table, Date date, int rowsCount)
    {
        for (int rowNumber = 1; rowNumber <= rowsCount; rowNumber++) {
            SimpleStatement insert = QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                    .value("key", literal("key " + rowNumber))
                    .value("typeuuid", literal(UUID.fromString(format("00000000-0000-0000-0000-%012d", rowNumber))))
                    .value("typetinyint", literal(rowNumber))
                    .value("typesmallint", literal(rowNumber))
                    .value("typeinteger", literal(rowNumber))
                    .value("typelong", literal(rowNumber + 1000))
                    .value("typebytes", literal(ByteBuffer.wrap(Ints.toByteArray(rowNumber)).asReadOnlyBuffer()))
                    .value("typedate", literal(LocalDate.ofInstant(Instant.ofEpochMilli(date.getTime()), ZoneId.systemDefault())))
                    .value("typetimestamp", literal(Instant.ofEpochMilli(date.getTime())))
                    .value("typeansi", literal("ansi " + rowNumber))
                    .value("typeboolean", literal(rowNumber % 2 == 0))
                    .value("typedecimal", literal(new BigDecimal(Math.pow(2, rowNumber))))
                    .value("typedouble", literal(Math.pow(4, rowNumber)))
                    .value("typefloat", literal((float) Math.pow(8, rowNumber)))
                    .value("typeinet", literal(InetAddresses.forString("127.0.0.1")))
                    .value("typevarchar", literal("varchar " + rowNumber))
                    .value("typevarint", literal(BigInteger.TEN.pow(rowNumber)))
                    .value("typetimeuuid", literal(UUID.fromString(format("d2177dd0-eaa2-11de-a572-001b779c76e%d", rowNumber))))
                    .value("typelist", literal(ImmutableList.of("list-value-1" + rowNumber, "list-value-2" + rowNumber)))
                    .value("typemap", literal(ImmutableMap.of(rowNumber, rowNumber + 1L, rowNumber + 2, rowNumber + 3L)))
                    .value("typeset", literal(ImmutableSet.of(false, true)))
                    .build();

            session.execute(insert);
        }
        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), rowsCount);
    }

    private static void createTableDeleteData(CassandraSession session, SchemaTableName table)
    {
        session.execute("DROP TABLE IF EXISTS " + table);
        session.execute("CREATE TABLE " + table + " (" +
                "partition_one bigint, " +
                "partition_two int, " +
                "clust_one text, " +
                "data text, " +
                "PRIMARY KEY((partition_one, partition_two), clust_one) " +
                ")");
        insertIntoTableDeleteData(session, table);
    }

    private static void insertIntoTableDeleteData(CassandraSession session, SchemaTableName table)
    {
        /*   This function inserts data (15 records) as below.
             partition_one | partition_two | clust_one   | data
            ---------------+---------------+-------------+------
                         1 |             1 | clust_one_1 | null
                         1 |             1 | clust_one_2 | null
                         1 |             1 | clust_one_3 | null
                         1 |             2 | clust_one_1 | null
                         1 |             2 | clust_one_2 | null
                         1 |             2 | clust_one_3 | null
                         2 |             2 | clust_one_1 | null
                         2 |             2 | clust_one_2 | null
                         3 |             3 | clust_one_3 | null
                         4 |             4 | clust_one_4 | null
                         5 |             5 | clust_one_5 | null
                         6 |             6 | clust_one_6 | null
                         7 |             7 | clust_one_7 | null
                         8 |             8 | clust_one_8 | null
                         9 |             9 | clust_one_9 | null
         */
        for (int rowNumber = 1; rowNumber < 10; rowNumber++) {
            SimpleStatement insert = QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                    .value("partition_one", literal(rowNumber))
                    .value("partition_two", literal(rowNumber))
                    .value("clust_one", literal("clust_one_" + rowNumber))
                    .build();
            session.execute(insert);
        }
        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", literal(1L)).value("partition_two", literal(1)).value("clust_one", literal("clust_one_" + 2)).build());
        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", literal(1L)).value("partition_two", literal(1)).value("clust_one", literal("clust_one_" + 3)).build());

        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", literal(1L)).value("partition_two", literal(2)).value("clust_one", literal("clust_one_" + 1)).build());
        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", literal(1L)).value("partition_two", literal(2)).value("clust_one", literal("clust_one_" + 2)).build());
        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", literal(1L)).value("partition_two", literal(2)).value("clust_one", literal("clust_one_" + 3)).build());

        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", literal(2L)).value("partition_two", literal(2)).value("clust_one", literal("clust_one_" + 1)).build());

        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), 15);
    }
}
