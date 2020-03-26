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
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import org.testng.SkipException;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.getResource;

public class TestOracleKerberosIntegrationSmokeTest
        extends BaseOracleIntegrationSmokeTest
{
    public TestOracleKerberosIntegrationSmokeTest()
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("oracle.authentication.type", "KERBEROS")
                        .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                        .put("kerberos.client.keytab", getResource("krb/client/test.keytab").toString())
                        .put("kerberos.config", getResource("krb/krb5.conf").toString())
                        .put("allow-drop-table", "true")
                        .build(),
                session -> Session.builder(session)
                        .setSchema("test")
                        .build(),
                ImmutableList.of(TpchTable.ORDERS, TpchTable.NATION)));
    }

    @Override
    protected String getUser()
    {
        return "test";
    }

    @Override
    public void testSelectInformationSchemaColumns()
    {
        throw new SkipException("This test is taking forever with kerberos authentication, due the connection retrying");
    }

    @Override
    public void testSelectInformationSchemaTables()
    {
        throw new SkipException("This test is taking forever with kerberos authentication, due the connection retrying");
    }
}
