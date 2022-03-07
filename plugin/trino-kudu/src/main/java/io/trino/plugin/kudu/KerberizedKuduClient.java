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
package io.trino.plugin.kudu;

import io.trino.plugin.base.authentication.CachingKerberosAuthentication;
import org.apache.kudu.client.KuduClient;

import javax.security.auth.Subject;

import java.security.PrivilegedAction;

import static java.util.Objects.requireNonNull;
import static org.apache.kudu.client.KuduClient.KuduClientBuilder;

public class KerberizedKuduClient
        extends ForwardingKuduClient
{
    private final KuduClient kuduClient;
    private final CachingKerberosAuthentication cachingKerberosAuthentication;

    KerberizedKuduClient(KuduClientBuilder kuduClientBuilder, CachingKerberosAuthentication cachingKerberosAuthentication)
    {
        requireNonNull(kuduClientBuilder, "kuduClientBuilder is null");
        this.cachingKerberosAuthentication = requireNonNull(cachingKerberosAuthentication, "cachingKerberosAuthentication is null");
        kuduClient = Subject.doAs(cachingKerberosAuthentication.getSubject(), (PrivilegedAction<KuduClient>) (kuduClientBuilder::build));
    }

    @Override
    protected KuduClient delegate()
    {
        cachingKerberosAuthentication.reauthenticateIfSoonWillBeExpired();
        return kuduClient;
    }
}
