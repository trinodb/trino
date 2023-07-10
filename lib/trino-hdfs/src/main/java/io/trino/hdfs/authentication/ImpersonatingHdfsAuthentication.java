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

import com.google.inject.Inject;
import io.trino.plugin.base.security.UserNameProvider;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.security.UserGroupInformation;

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
        return executeActionInDoAs(createProxyUser(userNameProvider.get(identity)), action);
    }

    private UserGroupInformation createProxyUser(String user)
    {
        return UserGroupInformation.createProxyUser(user, hadoopAuthentication.getUserGroupInformation());
    }
}
