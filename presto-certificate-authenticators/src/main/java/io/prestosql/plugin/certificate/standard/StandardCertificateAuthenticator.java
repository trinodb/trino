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
package io.prestosql.plugin.certificate.standard;

import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.CertificateAuthenticator;

import javax.inject.Inject;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.List;

public class StandardCertificateAuthenticator
        implements CertificateAuthenticator
{
    private final String name;

    @Inject
    public StandardCertificateAuthenticator()
    {
        this.name = "standard";
    }

    @Override
    public Principal authenticate(List<X509Certificate> certificates) throws AccessDeniedException
    {
        return certificates.get(0).getSubjectX500Principal();
    }
}
