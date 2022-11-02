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

import org.apache.hadoop.security.UserGroupInformation;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.authentication.KerberosTicketUtils.getRefreshTime;
import static io.trino.plugin.base.authentication.KerberosTicketUtils.getTicketGrantingTicket;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.UserGroupInformationShim.getSubject;

public class CachingKerberosHadoopAuthentication
        implements HadoopAuthentication
{
    private final KerberosHadoopAuthentication delegate;

    @GuardedBy("this")
    private UserGroupInformation userGroupInformation;
    @GuardedBy("this")
    private long nextRefreshTime;

    public CachingKerberosHadoopAuthentication(KerberosHadoopAuthentication delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public synchronized UserGroupInformation getUserGroupInformation()
    {
        if (nextRefreshTime < System.currentTimeMillis() || userGroupInformation == null) {
            userGroupInformation = requireNonNull(delegate.getUserGroupInformation(), "delegate.getUserGroupInformation() is null");
            nextRefreshTime = calculateNextRefreshTime(userGroupInformation);
        }
        return userGroupInformation;
    }

    private static long calculateNextRefreshTime(UserGroupInformation userGroupInformation)
    {
        Subject subject = getSubject(userGroupInformation);
        checkArgument(subject != null, "subject must be present in kerberos based UGI");
        KerberosTicket tgtTicket = getTicketGrantingTicket(subject);
        return getRefreshTime(tgtTicket);
    }
}
