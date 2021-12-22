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
package io.trino.tests.product.launcher.local;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestManuallyJdbcOauth2
{
    @BeforeClass(alwaysRun = true)
    public void verifyEtcHostsEntries()
            throws UnknownHostException
    {
        assertThat(InetAddress.getByName("presto-master").isLoopbackAddress()).isTrue();
        assertThat(InetAddress.getByName("hydra").isLoopbackAddress()).isTrue();
        assertThat(InetAddress.getByName("hydra-consent").isLoopbackAddress()).isTrue();

        assertThatNoException()
                .describedAs("Trino server is not available under 7778 port")
                .isThrownBy(() -> new Socket("presto-master", 7778).close());
    }

    /**
     * This test is here to allow manually tests OAuth2 implementation through jdbc.
     * It's configured in a way that allows it to connect to SinglenodeOauth2 environment. In order for it to work,
     * one must add to /etc/hosts following entries. They need to be removed before running automated tests against SinglenodeOAuth2* environments.
     * 127.0.0.1 presto-master
     * 127.0.0.1 hydra
     * 127.0.0.1 hydra-consent
     */
    @Test(enabled = false)
    public void shouldAuthenticateAndExecuteQuery()
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        String jdbcUrl = format("jdbc:trino://presto-master:7778?"
                + "SSL=true&"
                + "SSLTrustStorePath=%s&"
                + "SSLTrustStorePassword=123456&"
                + "externalAuthentication=true", TestManuallyJdbcOauth2.class.getResource("/docker/presto-product-tests/conf/presto/etc/presto-master.jks").getFile());
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
                PreparedStatement statement = connection.prepareStatement("select * from tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            assertThat(results.isClosed()).isFalse();
            assertThat(results.next()).isTrue();
        }
    }
}
