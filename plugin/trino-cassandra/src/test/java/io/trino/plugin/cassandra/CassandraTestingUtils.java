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

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;
import io.trino.spi.connector.SchemaTableName;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public final class CassandraTestingUtils
{
    public static final String TABLE_ALL_TYPES = "table_all_types";
    public static final String TABLE_ALL_TYPES_INSERT = "table_all_types_insert";
    public static final String TABLE_ALL_TYPES_PARTITION_KEY = "table_all_types_partition_key";
    public static final String TABLE_PUSHDOWN_UUID_PARTITION_KEY_PREDICATE = "table_pushdown_uuid_partition_key_predicate";
    public static final String TABLE_TUPLE_TYPE = "table_tuple_type";
    public static final String TABLE_USER_DEFINED_TYPE = "table_user_defined_type";
    public static final String TABLE_CLUSTERING_KEYS = "table_clustering_keys";
    public static final String TABLE_CLUSTERING_KEYS_LARGE = "table_clustering_keys_large";
    public static final String TABLE_MULTI_PARTITION_CLUSTERING_KEYS = "table_multi_partition_clustering_keys";
    public static final String TABLE_CLUSTERING_KEYS_INEQUALITY = "table_clustering_keys_inequality";
    public static final String TABLE_DELETE_DATA = "table_delete_data";

    private CassandraTestingUtils() {}

    public static void createTestTables(CassandraSession cassandraSession, String keyspace, Date date)
    {
        createKeyspace(cassandraSession, keyspace);
        createTableAllTypes(cassandraSession, new SchemaTableName(keyspace, TABLE_ALL_TYPES), date, 9);
        createTableAllTypes(cassandraSession, new SchemaTableName(keyspace, TABLE_ALL_TYPES_INSERT), date, 0);
        createTableAllTypesPartitionKey(cassandraSession, new SchemaTableName(keyspace, TABLE_ALL_TYPES_PARTITION_KEY), date);
        createTablePushdownUuidPartitionKey(cassandraSession, new SchemaTableName(keyspace, TABLE_PUSHDOWN_UUID_PARTITION_KEY_PREDICATE));
        createTableTupleType(cassandraSession, new SchemaTableName(keyspace, TABLE_TUPLE_TYPE));
        createTableUserDefinedType(cassandraSession, new SchemaTableName(keyspace, TABLE_USER_DEFINED_TYPE));
        createTableClusteringKeys(cassandraSession, new SchemaTableName(keyspace, TABLE_CLUSTERING_KEYS), 9);
        createTableClusteringKeys(cassandraSession, new SchemaTableName(keyspace, TABLE_CLUSTERING_KEYS_LARGE), 1000);
        createTableMultiPartitionClusteringKeys(cassandraSession, new SchemaTableName(keyspace, TABLE_MULTI_PARTITION_CLUSTERING_KEYS));
        createTableClusteringKeysInequality(cassandraSession, new SchemaTableName(keyspace, TABLE_CLUSTERING_KEYS_INEQUALITY), date, 4);
        createTableDeleteData(cassandraSession, new SchemaTableName(keyspace, TABLE_DELETE_DATA));
    }

    public static void createKeyspace(CassandraSession session, String keyspaceName)
    {
        session.execute("CREATE KEYSPACE " + keyspaceName + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
    }

    public static void createTableClusteringKeys(CassandraSession session, SchemaTableName table, int rowsCount)
    {
        session.execute("DROP TABLE IF EXISTS " + table);
        session.execute("CREATE TABLE " + table + " (" +
                "key text, " +
                "clust_one text, " +
                "clust_two text, " +
                "clust_three text, " +
                "data text, " +
                "PRIMARY KEY((key), clust_one, clust_two, clust_three) " +
                ")");
        insertIntoTableClusteringKeys(session, table, rowsCount);
    }

    public static void insertIntoTableClusteringKeys(CassandraSession session, SchemaTableName table, int rowsCount)
    {
        for (int rowNumber = 1; rowNumber <= rowsCount; rowNumber++) {
            Insert insert = QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                    .value("key", "key_" + rowNumber)
                    .value("clust_one", "clust_one")
                    .value("clust_two", "clust_two_" + rowNumber)
                    .value("clust_three", "clust_three_" + rowNumber);
            session.execute(insert);
        }
        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), rowsCount);
    }

    public static void createTableMultiPartitionClusteringKeys(CassandraSession session, SchemaTableName table)
    {
        session.execute("DROP TABLE IF EXISTS " + table);
        session.execute("CREATE TABLE " + table + " (" +
                "partition_one text, " +
                "partition_two text, " +
                "clust_one text, " +
                "clust_two text, " +
                "clust_three text, " +
                "data text, " +
                "PRIMARY KEY((partition_one, partition_two), clust_one, clust_two, clust_three) " +
                ")");
        insertIntoTableMultiPartitionClusteringKeys(session, table);
    }

    public static void insertIntoTableMultiPartitionClusteringKeys(CassandraSession session, SchemaTableName table)
    {
        for (int rowNumber = 1; rowNumber < 10; rowNumber++) {
            Insert insert = QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                    .value("partition_one", "partition_one_" + rowNumber)
                    .value("partition_two", "partition_two_" + rowNumber)
                    .value("clust_one", "clust_one")
                    .value("clust_two", "clust_two_" + rowNumber)
                    .value("clust_three", "clust_three_" + rowNumber);
            session.execute(insert);
        }
        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), 9);
    }

    public static void createTableClusteringKeysInequality(CassandraSession session, SchemaTableName table, Date date, int rowsCount)
    {
        session.execute("DROP TABLE IF EXISTS " + table);
        session.execute("CREATE TABLE " + table + " (" +
                "key text, " +
                "clust_one text, " +
                "clust_two int, " +
                "clust_three timestamp, " +
                "data text, " +
                "PRIMARY KEY((key), clust_one, clust_two, clust_three) " +
                ")");
        insertIntoTableClusteringKeysInequality(session, table, date, rowsCount);
    }

    public static void insertIntoTableClusteringKeysInequality(CassandraSession session, SchemaTableName table, Date date, int rowsCount)
    {
        for (int rowNumber = 1; rowNumber <= rowsCount; rowNumber++) {
            Insert insert = QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                    .value("key", "key_1")
                    .value("clust_one", "clust_one")
                    .value("clust_two", rowNumber)
                    .value("clust_three", date.getTime() + rowNumber * 10);
            session.execute(insert);
        }
        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), rowsCount);
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

    public static void createTableAllTypesPartitionKey(CassandraSession session, SchemaTableName table, Date date)
    {
        session.execute("DROP TABLE IF EXISTS " + table);

        session.execute("CREATE TABLE " + table + " (" +
                " key text, " +
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
                " typelist frozen <list<text>>, " +
                " typemap frozen <map<int, bigint>>, " +
                " typeset frozen <set<boolean>>, " +
                " PRIMARY KEY ((" +
                "   key, " +
                "   typeuuid, " +
                "   typetinyint, " +
                "   typesmallint, " +
                "   typeinteger, " +
                "   typelong, " +
                // TODO: NOT YET SUPPORTED AS A PARTITION KEY
                "   typebytes, " +
                "   typedate, " +
                "   typetimestamp, " +
                "   typeansi, " +
                "   typeboolean, " +
                // TODO: PRECISION LOST. IMPLEMENT IT AS STRING
                "   typedecimal, " +
                "   typedouble, " +
                "   typefloat, " +
                "   typeinet, " +
                "   typevarchar, " +
                // TODO: NOT YET SUPPORTED AS A PARTITION KEY
                "   typevarint, " +
                "   typetimeuuid, " +
                // TODO: NOT YET SUPPORTED AS A PARTITION KEY
                "   typelist, " +
                "   typemap, " +
                "   typeset" +
                " ))" +
                ")");

        insertTestData(session, table, date, 9);
    }

    public static void createTablePushdownUuidPartitionKey(CassandraSession session, SchemaTableName table)
    {
        session.execute("DROP TABLE IF EXISTS " + table);

        session.execute("CREATE TABLE " + table + " (col_uuid uuid PRIMARY KEY, col_text text)");

        session.execute("INSERT INTO " + table + "(col_uuid, col_text) VALUES (00000000-0000-0000-0000-000000000001, 'Trino')");
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
            Insert insert = QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                    .value("key", "key " + rowNumber)
                    .value("typeuuid", UUID.fromString(format("00000000-0000-0000-0000-%012d", rowNumber)))
                    .value("typetinyint", rowNumber)
                    .value("typesmallint", rowNumber)
                    .value("typeinteger", rowNumber)
                    .value("typelong", rowNumber + 1000)
                    .value("typebytes", ByteBuffer.wrap(Ints.toByteArray(rowNumber)).asReadOnlyBuffer())
                    .value("typedate", LocalDate.fromMillisSinceEpoch(date.getTime()))
                    .value("typetimestamp", date)
                    .value("typeansi", "ansi " + rowNumber)
                    .value("typeboolean", rowNumber % 2 == 0)
                    .value("typedecimal", new BigDecimal(Math.pow(2, rowNumber)))
                    .value("typedouble", Math.pow(4, rowNumber))
                    .value("typefloat", (float) Math.pow(8, rowNumber))
                    .value("typeinet", InetAddresses.forString("127.0.0.1"))
                    .value("typevarchar", "varchar " + rowNumber)
                    .value("typevarint", BigInteger.TEN.pow(rowNumber))
                    .value("typetimeuuid", UUID.fromString(format("d2177dd0-eaa2-11de-a572-001b779c76e%d", rowNumber)))
                    .value("typelist", ImmutableList.of("list-value-1" + rowNumber, "list-value-2" + rowNumber))
                    .value("typemap", ImmutableMap.of(rowNumber, rowNumber + 1L, rowNumber + 2, rowNumber + 3L))
                    .value("typeset", ImmutableSet.of(false, true));

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
            Insert insert = QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                    .value("partition_one", rowNumber)
                    .value("partition_two", rowNumber)
                    .value("clust_one", "clust_one_" + rowNumber);
            session.execute(insert);
        }
        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", 1L).value("partition_two", 1).value("clust_one", "clust_one_" + 2));
        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", 1L).value("partition_two", 1).value("clust_one", "clust_one_" + 3));

        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", 1L).value("partition_two", 2).value("clust_one", "clust_one_" + 1));
        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", 1L).value("partition_two", 2).value("clust_one", "clust_one_" + 2));
        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", 1L).value("partition_two", 2).value("clust_one", "clust_one_" + 3));

        session.execute(QueryBuilder.insertInto(table.getSchemaName(), table.getTableName())
                .value("partition_one", 2L).value("partition_two", 2).value("clust_one", "clust_one_" + 1));

        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), 15);
    }
}
