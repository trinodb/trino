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

import javax.security.auth.login.LoginException;

import static java.util.Objects.requireNonNull;
import static org.ietf.jgss.GSSContext.INDEFINITE_LIFETIME;

public class DelegatedConstrainedContextProvider
        implements GSSContextProvider
{
    protected static final GSSManager GSS_MANAGER = GSSManager.getInstance();

    private final GSSCredential gssCredential;

    public DelegatedConstrainedContextProvider(GSSCredential gssCredential)
    {
        this.gssCredential = requireNonNull(gssCredential, "gssCredential is null");
    }

    @Override
    public GSSContext getContext(String servicePrincipal, Duration minCredentialLifetime)
            throws GSSException, LoginException
    {
        return GSS_MANAGER.createContext(
                GSS_MANAGER.createName(servicePrincipal, null),
                SPNEGO_OID,
                gssCredential,
                INDEFINITE_LIFETIME);
    }
}
