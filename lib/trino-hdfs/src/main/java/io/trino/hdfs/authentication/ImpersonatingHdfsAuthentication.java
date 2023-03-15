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
package io.trino.hdfs.authentication;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import io.trino.plugin.base.security.UserNameProvider;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.UserGroupInformation;

import javax.inject.Inject;

import static io.trino.hdfs.authentication.UserGroupInformationUtils.executeActionInDoAs;
import static java.util.Objects.requireNonNull;

public class ImpersonatingHdfsAuthentication
        implements HdfsAuthentication
{
    private final HadoopAuthentication hadoopAuthentication;
    private final UserNameProvider userNameProvider;

    @Inject
    public ImpersonatingHdfsAuthentication(@ForHdfs HadoopAuthentication hadoopAuthentication, @ForHdfs UserNameProvider userNameProvider)
    {
        this.hadoopAuthentication = requireNonNull(hadoopAuthentication);
        this.userNameProvider = requireNonNull(userNameProvider);
    }

    @Override
    public <R, E extends Exception> R doAs(ConnectorIdentity identity, GenericExceptionAction<R, E> action)
            throws E
    {
        return executeActionInDoAs(createProxyUser(userNameProvider.get(identity), false), action);
    }

    @Override
    public <R, E extends Exception> R idempotentDoAs(ConnectorIdentity identity, GenericExceptionAction<R, E> action)
            throws E
    {
        int retries = 0;
        int maxRetries = 4;
        E lastException;
        UserGroupInformation currentUGI = createProxyUser(userNameProvider.get(identity), false);

        do {
            try {
                return executeActionInDoAs(currentUGI, action);
            }
            catch (Exception exception) {
                if (!isKerberosException(exception)) {
                    throw exception;
                }
                currentUGI = createProxyUser(userNameProvider.get(identity), true);
                try {
                    Thread.sleep(1_000);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                lastException = (E) exception;
            }
        } while (retries++ < maxRetries);

        throw lastException;
    }

    private UserGroupInformation createProxyUser(String user, boolean forceRefresh)
    {
        return UserGroupInformation.createProxyUser(user, hadoopAuthentication.getUserGroupInformation(forceRefresh));
    }

    private static boolean isKerberosException(Exception e)
    {
        return Iterables.tryFind(Throwables.getCausalChain(e), value -> value instanceof AccessControlException || value instanceof KerberosAuthException)
                .isPresent();
    }
}
