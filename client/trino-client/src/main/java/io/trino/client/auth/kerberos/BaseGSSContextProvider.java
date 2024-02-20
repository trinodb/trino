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
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ietf.jgss.GSSContext.INDEFINITE_LIFETIME;
import static org.ietf.jgss.GSSName.NT_HOSTBASED_SERVICE;

public abstract class BaseGSSContextProvider
        implements GSSContextProvider
{
    protected static final GSSManager GSS_MANAGER = GSSManager.getInstance();
    protected static final Oid SPNEGO_OID = createOid("1.3.6.1.5.5.2");
    protected static final Oid KERBEROS_OID = createOid("1.2.840.113554.1.2.2");
    protected static final Duration MIN_CREDENTIAL_LIFETIME = new Duration(60, SECONDS);

    protected GSSContext createContext(String servicePrincipal, GSSCredential gssCredential)
            throws GSSException
    {
        GSSContext result = GSS_MANAGER.createContext(
                GSS_MANAGER.createName(servicePrincipal, NT_HOSTBASED_SERVICE),
                SPNEGO_OID,
                gssCredential,
                INDEFINITE_LIFETIME);

        result.requestMutualAuth(true);
        result.requestConf(true);
        result.requestInteg(true);
        result.requestCredDeleg(true);
        return result;
    }

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
