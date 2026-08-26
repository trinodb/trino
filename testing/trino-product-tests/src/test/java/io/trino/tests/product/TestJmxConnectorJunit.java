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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.jdbc.JdbcBasicEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static java.sql.Types.BIGINT;
import static java.sql.Types.VARCHAR;

@ProductTest
@RequiresEnvironment(JdbcBasicEnvironment.class)
@TestGroup.Jdbc
class TestJmxConnectorJunit
{
    @Test
    void selectFromJavaRuntimeJmxMBean(JdbcBasicEnvironment env)
    {
        assertThat(env.executeTrino("SELECT node, vmname, vmversion FROM jmx.current.\"java.lang:type=runtime\""))
                .hasColumns(List.of(VARCHAR, VARCHAR, VARCHAR))
                .hasAnyRows();
    }

    @Test
    void selectFromJavaOperatingSystemJmxMBean(JdbcBasicEnvironment env)
    {
        assertThat(env.executeTrino("SELECT openfiledescriptorcount, maxfiledescriptorcount FROM jmx.current.\"java.lang:type=operatingsystem\""))
                .hasColumns(List.of(BIGINT, BIGINT))
                .hasAnyRows();
    }
}
