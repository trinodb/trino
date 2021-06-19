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
package okhttp3.internal.tls;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import static okhttp3.internal.Util.verifyAsIpAddress;
import static okhttp3.internal.tls.OkHostnameVerifier.allSubjectAltNames;

public class LegacyHostnameVerifier
        implements HostnameVerifier
{
    public static final HostnameVerifier INSTANCE = new LegacyHostnameVerifier();

    private LegacyHostnameVerifier() {}

    @Override
    public boolean verify(String host, SSLSession session)
    {
        if (OkHostnameVerifier.INSTANCE.verify(host, session)) {
            return true;
        }

        // the CN cannot be used with IP addresses
        if (verifyAsIpAddress(host)) {
            return false;
        }

        // try to verify using the legacy CN rules
        try {
            Certificate[] certificates = session.getPeerCertificates();
            X509Certificate certificate = (X509Certificate) certificates[0];

            // only use CN if there are no alt names
            if (!allSubjectAltNames(certificate).isEmpty()) {
                return false;
            }

            X500Principal principal = certificate.getSubjectX500Principal();
            // RFC 2818 advises using the most specific name for matching.
            String cn = new DistinguishedNameParser(principal).findMostSpecific("cn");
            if (cn != null) {
                return OkHostnameVerifier.INSTANCE.verifyHostname(host, cn);
            }

            return false;
        }
        catch (SSLException e) {
            return false;
        }
    }
}
