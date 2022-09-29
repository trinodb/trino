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

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class KerberosAuthentication
{
    private static final Logger log = Logger.get(KerberosAuthentication.class);

    private final KerberosPrincipal principal;
    private final Configuration configuration;

    public KerberosAuthentication(KerberosConfiguration kerberosConfiguration)
    {
        requireNonNull(kerberosConfiguration, "kerberosConfiguration is null");
        this.principal = kerberosConfiguration.kerberosPrincipal();
        if (log.isDebugEnabled()) {
            kerberosConfiguration = kerberosConfiguration.withDebug();
        }
        this.configuration = kerberosConfiguration.getConfiguration();
    }

    public Subject getSubject()
    {
        Subject subject = new Subject(false, ImmutableSet.of(principal), emptySet(), emptySet());
        try {
            LoginContext loginContext = new LoginContext("", subject, null, configuration);
            loginContext.login();
            return loginContext.getSubject();
        }
        catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    public void attemptLogin(Subject subject)
    {
        try {
            synchronized (subject.getPrivateCredentials()) {
                subject.getPrivateCredentials().clear();
                LoginContext loginContext = new LoginContext("", subject, null, configuration);
                loginContext.login();
            }
        }
        catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }
}
