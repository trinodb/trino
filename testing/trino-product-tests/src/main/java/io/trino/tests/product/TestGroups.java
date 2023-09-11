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

public final class TestGroups
{
    public static final String CREATE_TABLE = "create_table";
    public static final String CREATE_DROP_VIEW = "create_drop_view";
    public static final String ALTER_TABLE = "alter_table";
    public static final String COMMENT = "comment";
    public static final String SIMPLE = "simple";
    public static final String FUNCTIONS = "functions";
    public static final String CLI = "cli";
    public static final String SYSTEM_CONNECTOR = "system";
    public static final String CONFIGURED_FEATURES = "configured_features";
    public static final String JMX_CONNECTOR = "jmx";
    public static final String BLACKHOLE_CONNECTOR = "blackhole";
    public static final String SMOKE = "smoke";
    public static final String JDBC = "jdbc";
    public static final String JDBC_KERBEROS_CONSTRAINED_DELEGATION = "jdbc_kerberos_constrained_delegation";
    public static final String OAUTH2 = "oauth2";
    public static final String OAUTH2_REFRESH = "oauth2_refresh";
    public static final String MYSQL = "mysql";
    public static final String TRINO_JDBC = "trino_jdbc";
    public static final String QUERY_ENGINE = "qe";
    public static final String COMPARISON = "comparison";
    public static final String LOGICAL = "logical";
    public static final String JSON_FUNCTIONS = "json_functions";
    public static final String STORAGE_FORMATS = "storage_formats";
    public static final String STORAGE_FORMATS_DETAILED = "storage_formats_detailed";
    public static final String HMS_ONLY = "hms_only";
    public static final String PROFILE_SPECIFIC_TESTS = "profile_specific_tests";
    public static final String HDFS_IMPERSONATION = "hdfs_impersonation";
    public static final String HDFS_NO_IMPERSONATION = "hdfs_no_impersonation";
    public static final String HIVE_PARTITIONING = "hive_partitioning";
    public static final String HIVE_SPARK = "hive_spark";
    public static final String HIVE_SPARK_NO_STATS_FALLBACK = "hive_spark_no_stats_fallback";
    public static final String HIVE_COMPRESSION = "hive_compression";
    public static final String HIVE_TRANSACTIONAL = "hive_transactional";
    public static final String HIVE_VIEWS = "hive_views";
    public static final String HIVE_VIEW_COMPATIBILITY = "hive_view_compatibility";
    public static final String HIVE_CACHING = "hive_caching";
    public static final String HIVE_ICEBERG_REDIRECTIONS = "hive_iceberg_redirections";
    public static final String HIVE_HUDI_REDIRECTIONS = "hive_hudi_redirections";
    public static final String HIVE_KERBEROS = "hive_kerberos";
    public static final String AUTHORIZATION = "authorization";
    public static final String HIVE_COERCION = "hive_coercion";
    public static final String AZURE = "azure";
    public static final String CASSANDRA = "cassandra";
    public static final String SQL_SERVER = "sqlserver";
    public static final String LDAP = "ldap";
    public static final String LDAP_AND_FILE = "ldap_and_file";
    public static final String LDAP_CLI = "ldap_cli";
    public static final String LDAP_AND_FILE_CLI = "ldap_and_file_cli";
    public static final String LDAP_MULTIPLE_BINDS = "ldap_multiple_binds";
    public static final String HDP3_ONLY = "hdp3_only";
    public static final String TLS = "tls";
    public static final String ROLES = "roles";
    public static final String CANCEL_QUERY = "cancel_query";
    public static final String LARGE_QUERY = "large_query";
    public static final String KAFKA = "kafka";
    public static final String KAFKA_CONFLUENT_LICENSE = "kafka_confluent_license";
    public static final String TWO_HIVES = "two_hives";
    public static final String ICEBERG = "iceberg";
    public static final String ICEBERG_FORMAT_VERSION_COMPATIBILITY = "iceberg_format_version_compatibility";
    public static final String ICEBERG_REST = "iceberg_rest";
    public static final String ICEBERG_JDBC = "iceberg_jdbc";
    public static final String ICEBERG_NESSIE = "iceberg_nessie";
    public static final String AVRO = "avro";
    public static final String PHOENIX = "phoenix";
    public static final String CLICKHOUSE = "clickhouse";
    public static final String KUDU = "kudu";
    public static final String MARIADB = "mariadb";
    public static final String DELTA_LAKE_OSS = "delta-lake-oss";
    public static final String DELTA_LAKE_HDFS = "delta-lake-hdfs";
    public static final String DELTA_LAKE_MINIO = "delta-lake-minio";
    public static final String DELTA_LAKE_GCS = "delta-lake-gcs";
    public static final String DELTA_LAKE_DATABRICKS = "delta-lake-databricks";
    public static final String DELTA_LAKE_EXCLUDE_91 = "delta-lake-exclude-91";
    public static final String DELTA_LAKE_EXCLUDE_104 = "delta-lake-exclude-104";
    public static final String DELTA_LAKE_EXCLUDE_113 = "delta-lake-exclude-113";
    public static final String DELTA_LAKE_EXCLUDE_122 = "delta-lake-exclude-122";
    public static final String DELTA_LAKE_EXCLUDE_133 = "delta-lake-exclude-133";
    public static final String HUDI = "hudi";
    public static final String PARQUET = "parquet";
    public static final String IGNITE = "ignite";

    private TestGroups() {}
}
