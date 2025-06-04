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
import org.ietf.jgss.GSSException;

import javax.security.auth.RefreshFailedException;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;

public class DelegatedUnconstrainedContextProvider
        extends AbstractUnconstrainedContextProvider
{
    private final Subject subject = current();

    @Override
    protected Subject getSubject()
    {
        return subject;
    }

    @Override
    public void refresh()
            throws GSSException
    {
        Set<KerberosTicket> credentials = subject.getPrivateCredentials(KerberosTicket.class);

        if (credentials.size() > 1) {
            throw new ClientException("Invalid Credentials. Multiple Kerberos Credentials found.");
        }
        KerberosTicket kerberosTicket = getOnlyElement(credentials);
        if (kerberosTicket.isRenewable()) {
            try {
                kerberosTicket.refresh();
            }
            catch (RefreshFailedException exception) {
                throw new ClientException("Unable to refresh the kerberos ticket", exception);
            }
        }
    }

    private static Subject current()
    {
        try {
            Method current = Subject.class.getDeclaredMethod("current");
            return (Subject) current.invoke(null);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ignored) {
            // Fallback to pre-Java 17 method
            return Subject.getSubject(AccessController.getContext());
        }
    }
}
