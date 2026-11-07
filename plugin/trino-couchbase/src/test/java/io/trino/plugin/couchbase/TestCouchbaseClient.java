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
package io.trino.plugin.couchbase;

import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.security.cert.X509Certificate;
import java.util.List;

import static io.trino.plugin.couchbase.CouchbaseClient.createAuthenticator;
import static io.trino.plugin.couchbase.CouchbaseClient.loadClientCertificateChain;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCouchbaseClient
{
    private static final String CA_CERTIFICATE = resource("tls/ca.crt");
    private static final String CLIENT_CERTIFICATE = resource("tls/client.crt");
    private static final String CLIENT_KEY = resource("tls/client.key");

    @Test
    void testClientCertificateChainComesFromClientCertificate()
            throws Exception
    {
        List<X509Certificate> chain = loadClientCertificateChain(new CouchbaseConfig()
                .setTlsCertificate(CA_CERTIFICATE)
                .setTlsClientCertificate(CLIENT_CERTIFICATE));

        assertThat(chain).hasSize(1);
        assertThat(chain.get(0).getSubjectX500Principal().getName()).isEqualTo("CN=test-client");
    }

    @Test
    void testClusterCertificateIsNotUsedAsClientCertificateChain()
            throws Exception
    {
        List<X509Certificate> chain = loadClientCertificateChain(new CouchbaseConfig()
                .setTlsCertificate(CA_CERTIFICATE));

        assertThat(chain).isEmpty();
    }

    @Test
    void testCertificateAuthenticatorWhenKeyIsSet()
    {
        assertThat(createAuthenticator(new CouchbaseConfig()
                .setTlsKey(CLIENT_KEY)
                .setTlsCertificate(CA_CERTIFICATE)
                .setTlsClientCertificate(CLIENT_CERTIFICATE)))
                .isInstanceOf(CertificateAuthenticator.class);
    }

    @Test
    void testPasswordAuthenticatorWhenKeyIsNotSet()
    {
        assertThat(createAuthenticator(new CouchbaseConfig()
                .setUsername("some-user")
                .setPassword("some-password")))
                .isInstanceOf(PasswordAuthenticator.class);
    }

    private static String resource(String name)
    {
        return new File(TestCouchbaseClient.class.getClassLoader().getResource(name).getFile()).getAbsolutePath();
    }
}
