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
package io.trino.plugin.base.authentication;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import static io.trino.plugin.base.authentication.KerberosTicketUtils.getTicketGrantingTicket;
import static java.util.Objects.requireNonNull;

public class CachingKerberosAuthentication
{
    private final KerberosAuthentication kerberosAuthentication;

    @GuardedBy("this")
    private LoginContext loginContext;

    @GuardedBy("this")
    private long nextRefreshTime;

    public CachingKerberosAuthentication(KerberosAuthentication kerberosAuthentication)
    {
        this.kerberosAuthentication = requireNonNull(kerberosAuthentication, "kerberosAuthentication is null");
    }

    public synchronized Subject getSubject()
    {
        if (loginContext == null || ticketNeedsRefresh()) {
            loginContext = kerberosAuthentication.getLoginContext();
            Subject subject = getRequiredSubject();
            KerberosTicket tgtTicket = getTicketGrantingTicket(subject);
            nextRefreshTime = KerberosTicketUtils.getRefreshTime(tgtTicket);
            return subject;
        }
        return getRequiredSubject();
    }

    public synchronized void reauthenticateIfSoonWillBeExpired()
    {
        requireNonNull(loginContext, "loginContext is null. getSubject must be called before reauthenticateIfSoonWillBeExpired");
        if (ticketNeedsRefresh()) {
            Subject subject = getRequiredSubject();
            try {
                loginContext.logout();
                loginContext = kerberosAuthentication.loginFromSubject(subject);
            }
            catch (LoginException e) {
                throw new RuntimeException(e);
            }
            KerberosTicket tgtTicket = getTicketGrantingTicket(subject);
            nextRefreshTime = KerberosTicketUtils.getRefreshTime(tgtTicket);
        }
    }

    private boolean ticketNeedsRefresh()
    {
        return nextRefreshTime < System.currentTimeMillis();
    }

    private Subject getRequiredSubject()
    {
        return requireNonNull(loginContext.getSubject(), "loginContext.getSubject() is null");
    }
}
