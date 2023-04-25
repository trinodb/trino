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

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ietf.jgss.GSSContext.INDEFINITE_LIFETIME;
import static org.ietf.jgss.GSSCredential.DEFAULT_LIFETIME;
import static org.ietf.jgss.GSSCredential.INITIATE_ONLY;
import static org.ietf.jgss.GSSName.NT_USER_NAME;

public abstract class AbstractSubjectBasedContextProvider
        implements GSSContextProvider
{
    protected static final GSSManager GSS_MANAGER = GSSManager.getInstance();
    private GSSCredential clientCredential;

    @Override
    public GSSContext getContext(String servicePrincipal, Duration minCredentialLifetime)
            throws GSSException, LoginException
    {
        if ((clientCredential == null) || clientCredential.getRemainingLifetime() < minCredentialLifetime.getValue(SECONDS)) {
            clientCredential = createGssCredential();
        }

        return doAs(getSubject(), () -> {
            GSSContext result = GSS_MANAGER.createContext(
                    // http://web.archive.org/web/20091008014853/http://blogs.sun.com/harcey/entry/of_java_kerberos_and_access
                    // We pass null to indicate that the system should interpret the SPN
                    // as it is.
                    GSS_MANAGER.createName(servicePrincipal, null),
                    SPNEGO_OID,
                    clientCredential,
                    INDEFINITE_LIFETIME);

            result.requestMutualAuth(true);
            result.requestConf(true);
            result.requestInteg(true);
            result.requestCredDeleg(true);
            return result;
        });
    }

    private GSSCredential createGssCredential() throws GSSException, LoginException
    {
        refresh();
        Subject subject = getSubject();
        Principal clientPrincipal = subject.getPrincipals().iterator().next();
        return doAs(subject, () -> GSS_MANAGER.createCredential(
                GSS_MANAGER.createName(clientPrincipal.getName(), NT_USER_NAME),
                DEFAULT_LIFETIME,
                KERBEROS_OID,
                INITIATE_ONLY));
    }

    public abstract void refresh() throws LoginException, GSSException;

    protected abstract Subject getSubject();

    interface GssSupplier<T>
    {
        T get() throws GSSException;
    }

    static <T> T doAs(Subject subject, GssSupplier<T> action)
            throws GSSException
    {
        try {
            return Subject.doAs(subject, (PrivilegedExceptionAction<T>) action::get);
        }
        catch (PrivilegedActionException e) {
            Throwable t = e.getCause();
            throwIfInstanceOf(t, GSSException.class);
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }
}
