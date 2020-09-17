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
package io.prestosql.security;

import io.prestosql.plugin.base.security.AllowAllSystemAccessControl;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.spi.security.SystemSecurityContext;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.security.AccessDeniedException.denyImpersonateUser;
import static java.util.Objects.requireNonNull;

/**
 * Default system access control rules.
 * By default all access is allowed except for user impersonation.
 */
public class DefaultSystemAccessControl
        extends AllowAllSystemAccessControl
{
    public static final String NAME = "default";

    private static final DefaultSystemAccessControl INSTANCE = new DefaultSystemAccessControl();

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");
            checkArgument(config.isEmpty(), "This access controller does not support any configuration properties");
            return INSTANCE;
        }
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName)
    {
        denyImpersonateUser(context.getIdentity().getUser(), userName);
    }
}
