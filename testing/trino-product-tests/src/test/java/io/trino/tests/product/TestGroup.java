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

import org.junit.jupiter.api.Tag;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Test group annotations for JUnit 5 product tests.
 * <p>
 * Usage:
 * <pre>
 * {@code
 * @TestGroup.Mysql
 * @TestGroup.ProfileSpecificTests
 * class TestMySqlConnector {
 *     @Test
 *     void testQuery() { ... }
 * }
 * }
 * </pre>
 * <p>
 * These annotations are meta-annotations that include {@link Tag}, so they work
 * with JUnit 5's tag filtering: {@code mvn test -Dgroups=mysql}
 */
public @interface TestGroup
{
    // Core functionality
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("functions")
    @interface Functions {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("cli")
    @interface Cli {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("configured_features")
    @interface ConfiguredFeatures {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("tpch")
    @interface Tpch {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("tpcds")
    @interface Tpcds {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("join")
    @interface Join {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("group-by")
    @interface GroupBy {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("smoke")
    @interface Smoke {}

    // JDBC
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("jdbc")
    @interface Jdbc {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("jdbc_kerberos_constrained_delegation")
    @interface JdbcKerberosConstrainedDelegation {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("trino_jdbc")
    @interface TrinoJdbc {}

    // Authentication
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("oauth2")
    @interface Oauth2 {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("oauth2_refresh")
    @interface Oauth2Refresh {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("ldap")
    @interface Ldap {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("ldap_and_file")
    @interface LdapAndFile {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("ldap_cli")
    @interface LdapCli {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("ldap_and_file_cli")
    @interface LdapAndFileCli {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("ldap_multiple_binds")
    @interface LdapMultipleBinds {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("tls")
    @interface Tls {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("authorization")
    @interface Authorization {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("ranger")
    @interface Ranger {}

    // JDBC Connectors
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("mysql")
    @interface Mysql {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("postgresql")
    @interface Postgresql {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("sqlserver")
    @interface Sqlserver {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("clickhouse")
    @interface Clickhouse {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("mariadb")
    @interface Mariadb {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("exasol")
    @interface Exasol {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("ignite")
    @interface Ignite {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("snowflake")
    @interface Snowflake {}

    // NoSQL Connectors
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("cassandra")
    @interface Cassandra {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("kafka")
    @interface Kafka {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("kafka_confluent_license")
    @interface KafkaConfluentLicense {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("loki")
    @interface Loki {}

    // Blackhole
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("blackhole")
    @interface Blackhole {}

    // Spooling / Schema Registry subtags
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("postgresql_spooling")
    @interface PostgresqlSpooling {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("kafka_schema_registry")
    @interface KafkaSchemaRegistry {}

    // Hive
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("storage_formats")
    @interface StorageFormats {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("storage_formats_detailed")
    @interface StorageFormatsDetailed {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hms_only")
    @interface HmsOnly {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hdfs_impersonation")
    @interface HdfsImpersonation {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hdfs_no_impersonation")
    @interface HdfsNoImpersonation {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_gcs")
    @interface HiveGcs {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive4")
    @interface Hive4 {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_spark")
    @interface HiveSpark {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_spark_no_stats_fallback")
    @interface HiveSparkNoStatsFallback {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_compression")
    @interface HiveCompression {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_transactional")
    @interface HiveTransactional {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_view_compatibility")
    @interface HiveViewCompatibility {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_alluxio_caching")
    @interface HiveAlluxioCaching {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_iceberg_redirections")
    @interface HiveIcebergRedirections {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_hudi_redirections")
    @interface HiveHudiRedirections {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_kerberos")
    @interface HiveKerberos {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hive_file_header")
    @interface HiveFileHeader {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("two_hives")
    @interface TwoHives {}

    // Iceberg
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("iceberg")
    @interface Iceberg {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("iceberg_gcs")
    @interface IcebergGcs {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("iceberg_azure")
    @interface IcebergAzure {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("iceberg_alluxio_caching")
    @interface IcebergAlluxioCaching {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("iceberg_format_version_compatibility")
    @interface IcebergFormatVersionCompatibility {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("iceberg_rest")
    @interface IcebergRest {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("iceberg_jdbc")
    @interface IcebergJdbc {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("iceberg_nessie")
    @interface IcebergNessie {}

    // Delta Lake
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-oss")
    @interface DeltaLakeOss {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-hdfs")
    @interface DeltaLakeHdfs {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-minio")
    @interface DeltaLakeMinio {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-azure")
    @interface DeltaLakeAzure {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-gcs")
    @interface DeltaLakeGcs {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-databricks")
    @interface DeltaLakeDatabricks {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-databricks-133")
    @interface DeltaLakeDatabricks133 {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-databricks-143")
    @interface DeltaLakeDatabricks143 {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-databricks-154")
    @interface DeltaLakeDatabricks154 {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-databricks-164")
    @interface DeltaLakeDatabricks164 {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-exclude-173")
    @interface DeltaLakeExclude173 {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("delta-lake-alluxio-caching")
    @interface DeltaLakeAlluxioCaching {}

    // Other data lake formats
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("hudi")
    @interface Hudi {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("parquet")
    @interface Parquet {}

    // Cloud
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("azure")
    @interface Azure {}

    // Test categories
    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("profile_specific_tests")
    @interface ProfileSpecificTests {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("large_query")
    @interface LargeQuery {}

    @Target({TYPE, METHOD})
    @Retention(RUNTIME)
    @Tag("fault-tolerant")
    @interface FaultTolerant {}
}
