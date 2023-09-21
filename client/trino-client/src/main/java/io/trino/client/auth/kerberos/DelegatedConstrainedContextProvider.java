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

import io.trino.client.ClientException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DelegatedConstrainedContextProvider
        extends BaseGSSContextProvider
{
    private final GSSCredential gssCredential;

    public DelegatedConstrainedContextProvider(GSSCredential gssCredential)
    {
        this.gssCredential = requireNonNull(gssCredential, "gssCredential is null");
    }

    @Override
    public GSSContext getContext(String servicePrincipal)
            throws GSSException
    {
        if (gssCredential.getRemainingLifetime() < MIN_CREDENTIAL_LIFETIME.getValue(SECONDS)) {
            throw new ClientException(format("Kerberos credential is expired: %s seconds", gssCredential.getRemainingLifetime()));
        }
        return createContext(servicePrincipal, gssCredential);
    }
}
