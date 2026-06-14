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
package io.trino.plugin.exasol;

import com.google.common.net.HostAndPort;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.security.Principal;
import java.security.PublicKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestExasolTlsCertificateFingerprintProvider
{
    @Test
    void testExtractHostAndPortFromJdbcUrl()
    {
        assertExtractedHostAndPort("jdbc:exa:exa.example.com:8563", HostAndPort.fromParts("exa.example.com", 8563));
        assertExtractedHostAndPort("jdbc:exa:exa.example.com;schema=tpch", HostAndPort.fromParts("exa.example.com", 8563));
        assertExtractedHostAndPort("jdbc:exa-worker:worker.example.com", HostAndPort.fromParts("worker.example.com", 8563));
        assertExtractedHostAndPort("jdbc:exa:[2001:db8::1]:8564", HostAndPort.fromParts("2001:db8::1", 8564));
        assertExtractedHostAndPort("jdbc:exa:192.0.2.10", HostAndPort.fromParts("192.0.2.10", 8563));
        assertExtractedHostAndPort("jdbc:exa:192.0.2.10:8564", HostAndPort.fromParts("192.0.2.10", 8564));
        assertExtractedHostAndPort("jdbc:exa:exa1.example.com,exa2.example.com", HostAndPort.fromParts("exa1.example.com", 8563));
        assertExtractedHostAndPort("jdbc:exa:exa1.example.com:8564,exa2.example.com:8565", HostAndPort.fromParts("exa1.example.com", 8564));
        assertExtractedHostAndPort("jdbc:exa:10.0.0.11..14", HostAndPort.fromParts("10.0.0.11", 8563));
        assertExtractedHostAndPort("jdbc:exa:exa.example.com:8564/my_schema", HostAndPort.fromParts("exa.example.com", 8564));
    }

    @Test
    void testComputesUppercaseSha256Fingerprint()
    {
        assertThat(ExasolTlsCertificateFingerprintProvider.toSha256Fingerprint(new TestingX509Certificate(new byte[] {1, 2, 3})))
                .isEqualTo("039058C6F2C0CB492C533B0A4D14EF77CC0F78ABCCCED5287D84A1A2011CFB81");
    }

    @Test
    void testRejectsUnsupportedJdbcUrl()
    {
        assertThatThrownBy(() -> ExasolTlsCertificateFingerprintProvider.extractHostAndPort("jdbc:postgresql://exa.example.com:5432/db"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported Exasol JDBC URL: jdbc:postgresql://exa.example.com:5432/db");
    }

    private static void assertExtractedHostAndPort(String jdbcUrl, HostAndPort expected)
    {
        assertThat(ExasolTlsCertificateFingerprintProvider.extractHostAndPort(jdbcUrl))
                .isEqualTo(expected);
    }

    @SuppressWarnings("deprecation")
    private static final class TestingX509Certificate
            extends X509Certificate
    {
        private final byte[] encoded;

        private TestingX509Certificate(byte[] encoded)
        {
            this.encoded = encoded.clone();
        }

        @Override
        public byte[] getEncoded()
                throws CertificateEncodingException
        {
            return encoded.clone();
        }

        @Override
        public void verify(PublicKey key) {}

        @Override
        public void verify(PublicKey key, String sigProvider) {}

        @Override
        public String toString()
        {
            return "TestingX509Certificate";
        }

        @Override
        public PublicKey getPublicKey()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkValidity() {}

        @Override
        public void checkValidity(Date date) {}

        @Override
        public int getVersion()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BigInteger getSerialNumber()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Principal getIssuerDN()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Principal getSubjectDN()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date getNotBefore()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date getNotAfter()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getTBSCertificate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getSignature()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSigAlgName()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSigAlgOID()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getSigAlgParams()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean[] getIssuerUniqueID()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean[] getSubjectUniqueID()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean[] getKeyUsage()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getBasicConstraints()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getExtensionValue(String oid)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getNonCriticalExtensionOIDs()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getCriticalExtensionOIDs()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasUnsupportedCriticalExtension()
        {
            throw new UnsupportedOperationException();
        }
    }
}
