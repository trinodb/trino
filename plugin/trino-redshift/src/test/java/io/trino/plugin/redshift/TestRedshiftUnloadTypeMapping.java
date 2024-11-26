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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_ACCESS_KEY;
import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_REGION;
import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_SECRET_KEY;
import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.plugin.redshift.RedshiftQueryRunner.S3_UNLOAD_ROOT;
import static io.trino.plugin.redshift.TestingRedshiftServer.JDBC_PASSWORD;
import static io.trino.plugin.redshift.TestingRedshiftServer.JDBC_URL;
import static io.trino.plugin.redshift.TestingRedshiftServer.JDBC_USER;

public class TestRedshiftUnloadTypeMapping
        extends TestRedshiftTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("redshift.unload-location", S3_UNLOAD_ROOT)
                .put("redshift.unload-iam-role", IAM_ROLE)
                .put("s3.region", AWS_REGION)
                .put("s3.endpoint", "https://s3.%s.amazonaws.com".formatted(AWS_REGION))
                .put("s3.aws-access-key", AWS_ACCESS_KEY)
                .put("s3.aws-secret-key", AWS_SECRET_KEY)
                .put("connection-url", JDBC_URL)
                .put("connection-user", JDBC_USER)
                .put("connection-password", JDBC_PASSWORD)
                .put("s3.path-style-access", "true")
                .put("join-pushdown.enabled", "true")
                .put("join-pushdown.strategy", "EAGER")
                .buildOrThrow();

        return RedshiftQueryRunner.builder()
                .setConnectorProperties(properties)
                .build();
    }
}
