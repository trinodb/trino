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
package io.trino.client.auth.kerberos;

import io.airlift.units.Duration;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

import javax.security.auth.login.LoginException;

public interface GSSContextProvider
{
    Oid SPNEGO_OID = createOid("1.3.6.1.5.5.2");
    Oid KERBEROS_OID = createOid("1.2.840.113554.1.2.2");

    GSSContext getContext(String servicePrincipal, Duration minCredentialLifetime)
            throws GSSException, LoginException;

    static Oid createOid(String value)
    {
        try {
            return new Oid(value);
        }
        catch (GSSException e) {
            throw new AssertionError(e);
        }
    }
}
