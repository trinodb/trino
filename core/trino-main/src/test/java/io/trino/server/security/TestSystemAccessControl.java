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
package io.trino.server.security;

import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;

import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyReadSystemInformationAccess;

public class TestSystemAccessControl
        extends AllowAllSystemAccessControl
{
    public static final TestSystemAccessControl WITH_IMPERSONATION = new TestSystemAccessControl(false);
    public static final TestSystemAccessControl NO_IMPERSONATION = new TestSystemAccessControl(true);

    public static final String MANAGEMENT_USER = "management-user";

    private final boolean allowImpersonation;

    private TestSystemAccessControl(boolean allowImpersonation)
    {
        this.allowImpersonation = allowImpersonation;
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName)
    {
        if (!allowImpersonation) {
            denyImpersonateUser(context.getIdentity().getUser(), userName);
        }
    }

    @Override
    public void checkCanReadSystemInformation(SystemSecurityContext context)
    {
        if (!context.getIdentity().getUser().equals(MANAGEMENT_USER)) {
            denyReadSystemInformationAccess();
        }
    }
}
