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
package io.trino.tests.product.jdbc;

public class JdbcOAuth2HttpsProxyEnvironment
        extends AbstractJdbcOAuth2ProxyEnvironment
{
    @Override
    protected String getBaseConfigProperties()
    {
        return super.getBaseConfigProperties() +
                "oauth2-jwk.http-client.trust-store-path=/etc/trino/proxy-truststore.jks\n" +
                "oauth2-jwk.http-client.trust-store-password=123456\n" +
                "oauth2-jwk.http-client.http-proxy=proxy:8888\n" +
                "oauth2-jwk.http-client.http-proxy.secure=true\n" +
                "http-server.log.enabled=false\n";
    }

    @Override
    protected String getProxyHttpdConfResource()
    {
        return "oauth2/proxy/common/https-proxy/httpd.conf";
    }

    @Override
    protected String getProxyPemResource()
    {
        return "oauth2/proxy/common/https-proxy/proxy.pem";
    }

    @Override
    protected String getProxyTrustStoreResource()
    {
        return "oauth2/proxy/common/https-proxy/truststore.jks";
    }
}
