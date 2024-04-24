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
package io.trino.tests.product;

import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public final class TestGroups
{
    public static final String FUNCTIONS = "functions";
    public static final String CLI = "cli";
    public static final String CONFIGURED_FEATURES = "configured_features";
    public static final String TPCH = "tpch";
    public static final String TPCDS = "tpcds";
    // TODO some tests from this group are run in Suite1 and Suite3, probably not intentionally
    public static final String JOIN = "join";
    // TODO some tests from this group are run in Suite1 and Suite3, probably not intentionally
    public static final String GROUP_BY = "group-by";
    // TODO some tests from this group are run in Suite1 and Suite3, probably not intentionally
    public static final String SMOKE = "smoke";
    public static final String JDBC = "jdbc";
    public static final String JDBC_KERBEROS_CONSTRAINED_DELEGATION = "jdbc_kerberos_constrained_delegation";
    public static final String OAUTH2 = "oauth2";
    public static final String OAUTH2_REFRESH = "oauth2_refresh";
    public static final String MYSQL = "mysql";
    public static final String TRINO_JDBC = "trino_jdbc";
    public static final String STORAGE_FORMATS = "storage_formats";
    public static final String STORAGE_FORMATS_DETAILED = "storage_formats_detailed";
    public static final String HMS_ONLY = "hms_only";
    public static final String PROFILE_SPECIFIC_TESTS = "profile_specific_tests";
    public static final String HDFS_IMPERSONATION = "hdfs_impersonation";
    public static final String HDFS_NO_IMPERSONATION = "hdfs_no_impersonation";
    public static final String HIVE_SPARK = "hive_spark";
    public static final String HIVE_SPARK_NO_STATS_FALLBACK = "hive_spark_no_stats_fallback";
    public static final String HIVE_COMPRESSION = "hive_compression";
    public static final String HIVE_TRANSACTIONAL = "hive_transactional";
    public static final String HIVE_VIEW_COMPATIBILITY = "hive_view_compatibility";
    public static final String HIVE_ALLUXIO_CACHING = "hive_alluxio_caching";
    public static final String HIVE_ICEBERG_REDIRECTIONS = "hive_iceberg_redirections";
    public static final String HIVE_HUDI_REDIRECTIONS = "hive_hudi_redirections";
    public static final String HIVE_KERBEROS = "hive_kerberos";
    public static final String HIVE_FILE_HEADER = "hive_file_header";
    public static final String AUTHORIZATION = "authorization";
    public static final String AZURE = "azure";
    public static final String CASSANDRA = "cassandra";
    public static final String POSTGRESQL = "postgresql";
    public static final String SQLSERVER = "sqlserver";
    public static final String LDAP = "ldap";
    public static final String LDAP_AND_FILE = "ldap_and_file";
    public static final String LDAP_CLI = "ldap_cli";
    public static final String LDAP_AND_FILE_CLI = "ldap_and_file_cli";
    public static final String LDAP_MULTIPLE_BINDS = "ldap_multiple_binds";
    public static final String TLS = "tls";
    public static final String LARGE_QUERY = "large_query";
    public static final String KAFKA = "kafka";
    public static final String KAFKA_CONFLUENT_LICENSE = "kafka_confluent_license";
    public static final String TWO_HIVES = "two_hives";
    public static final String ICEBERG = "iceberg";
    public static final String ICEBERG_ALLUXIO_CACHING = "iceberg_alluxio_caching";
    public static final String ICEBERG_FORMAT_VERSION_COMPATIBILITY = "iceberg_format_version_compatibility";
    public static final String ICEBERG_REST = "iceberg_rest";
    public static final String ICEBERG_JDBC = "iceberg_jdbc";
    public static final String ICEBERG_NESSIE = "iceberg_nessie";
    public static final String PHOENIX = "phoenix";
    public static final String CLICKHOUSE = "clickhouse";
    public static final String KUDU = "kudu";
    public static final String MARIADB = "mariadb";
    public static final String SNOWFLAKE = "snowflake";
    public static final String DELTA_LAKE_OSS = "delta-lake-oss";
    public static final String DELTA_LAKE_HDFS = "delta-lake-hdfs";
    public static final String DELTA_LAKE_MINIO = "delta-lake-minio";
    public static final String DELTA_LAKE_GCS = "delta-lake-gcs";
    public static final String DELTA_LAKE_DATABRICKS = "delta-lake-databricks";
    public static final String DELTA_LAKE_DATABRICKS_104 = "delta-lake-databricks-104";
    public static final String DELTA_LAKE_DATABRICKS_113 = "delta-lake-databricks-113";
    public static final String DELTA_LAKE_DATABRICKS_122 = "delta-lake-databricks-122";
    public static final String DATABRICKS_UNITY_HTTP_HMS = "databricks-unity-http-hms";
    public static final String DELTA_LAKE_EXCLUDE_91 = "delta-lake-exclude-91";
    public static final String DELTA_LAKE_ALLUXIO_CACHING = "delta-lake-alluxio-caching";
    public static final String HUDI = "hudi";
    public static final String PARQUET = "parquet";
    public static final String IGNITE = "ignite";

    private TestGroups() {}

    public abstract static class Introspection
    {
        private Introspection() {}

        private static final Set<String> ALL_GROUPS;
        // Identity-based set
        private static final Set<String> ALL_GROUPS_IDENTITIES;

        static {
            try {
                ImmutableSet.Builder<String> groups = ImmutableSet.builder();
                Set<String> groupIdentities = newSetFromMap(new IdentityHashMap<>());
                for (Field field : TestGroups.class.getFields()) {
                    String group = (String) field.get(null);
                    groups.add(group);
                    groupIdentities.add(group);
                }
                ALL_GROUPS = groups.build();
                ALL_GROUPS_IDENTITIES = unmodifiableSet(groupIdentities);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        public static Set<String> getAllGroups()
        {
            return ALL_GROUPS;
        }

        public static void validateGroupIdentityReferences(Iterable<String> groupNames)
        {
            for (String groupName : groupNames) {
                checkArgument(
                        isGroupIdentityReference(groupName),
                        "Group name '%s' should reference one of the named constants in %s",
                        groupName,
                        TestGroups.class);
            }
        }

        public static boolean isGroupIdentityReference(String groupName)
        {
            return ALL_GROUPS_IDENTITIES.contains(requireNonNull(groupName, "groupName is null"));
        }
    }

    public static final class FakeUsageForMavenDependencyChecker
    {
        private FakeUsageForMavenDependencyChecker() {}

        public static void fakeUse() {}
    }
}
