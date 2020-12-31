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
package io.prestosql.spi.security;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.List;

public interface CertificateAuthenticator
{
    /**
     * Authenticate and extract principal from client certificate.
     *
     * @param certificates This client certificate chain, in ascending order of trust.
     * The first certificate in the chain is the one set by the client, the next is the
     * one used to authenticate the first, and so on.
     * @return the authenticated entity
     * @throws AccessDeniedException if not allowed
     */
    Principal authenticate(List<X509Certificate> certificates);
}
