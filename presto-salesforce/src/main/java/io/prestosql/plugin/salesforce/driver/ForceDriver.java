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
package io.prestosql.plugin.salesforce.driver;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.prestosql.plugin.salesforce.driver.connection.ForceConnection;
import io.prestosql.plugin.salesforce.driver.connection.ForceConnectionInfo;
import io.prestosql.plugin.salesforce.driver.connection.ForceService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ForceDriver
        implements Driver
{
    private static final String ACCEPTABLE_URL = "jdbc:salesforce";
    private static final Pattern URL_PATTERN = Pattern.compile("\\A" + ACCEPTABLE_URL + "://(.*)");
    private static final Pattern URL_HAS_AUTHORIZATION_SEGMENT = Pattern.compile("\\A" + ACCEPTABLE_URL + "://([^:]+):([^@]+)@.*");

    private static Boolean resolveSandboxProperty(Properties properties)
    {
        String sandbox = properties.getProperty("sandbox");
        if (sandbox != null) {
            return Boolean.valueOf(sandbox);
        }
        String loginDomain = properties.getProperty("loginDomain");
        if (loginDomain != null) {
            return loginDomain.contains("test");
        }
        return null;
    }

    @Override
    public Connection connect(String url, Properties properties)
            throws SQLException
    {
        if (!acceptsURL(url)) {
            /*
             * According to JDBC spec:
             * > The driver should return "null" if it realizes it is the wrong kind of driver to connect to the given URL.
             * > This will be common, as when the JDBC driver manager is asked to connect to a given URL it passes the URL to each loaded driver in turn.
             *
             * Source: https://docs.oracle.com/javase/8/docs/api/java/sql/Driver.html#connect-java.lang.String-java.util.Properties-
             */
            return null;
        }
        try {
            Properties connStringProps = getConnStringProperties(url);
            properties.putAll(connStringProps);
            ForceConnectionInfo info = new ForceConnectionInfo();
            info.setUserName(properties.getProperty("user"));
            info.setPassword(properties.getProperty("password"));
            info.setSessionId(properties.getProperty("sessionId"));
            info.setSandbox(resolveSandboxProperty(properties));

            PartnerConnection partnerConnection = ForceService.createPartnerConnection(info);
            return new ForceConnection(partnerConnection);
        }
        catch (ConnectionException | IOException e) {
            throw new SQLException(e);
        }
    }

    protected Properties getConnStringProperties(String url)
            throws IOException
    {
        Properties result = new Properties();
        String urlProperties = null;

        Matcher stdMatcher = URL_PATTERN.matcher(url);
        Matcher authMatcher = URL_HAS_AUTHORIZATION_SEGMENT.matcher(url);

        if (authMatcher.matches()) {
            urlProperties = "user=" + authMatcher.group(1) + "\npassword=" + authMatcher.group(2);
        }
        else if (stdMatcher.matches()) {
            urlProperties = stdMatcher.group(1);
            urlProperties = urlProperties.replaceAll(";", "\n");
        }

        if (urlProperties != null) {
            try (InputStream in = new ByteArrayInputStream(urlProperties.getBytes(StandardCharsets.UTF_8))) {
                result.load(in);
            }
        }

        return result;
    }

    @Override
    public boolean acceptsURL(String url)
    {
        return url != null && url.startsWith(ACCEPTABLE_URL);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
    {
        return new DriverPropertyInfo[] {};
    }

    @Override
    public int getMajorVersion()
    {
        return 1;
    }

    @Override
    public int getMinorVersion()
    {
        return 1;
    }

    @Override
    public boolean jdbcCompliant()
    {
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException();
    }

    static {
        try {
            DriverManager.registerDriver(new ForceDriver());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed register ForceDriver: " + e.getMessage(), e);
        }
    }
}
