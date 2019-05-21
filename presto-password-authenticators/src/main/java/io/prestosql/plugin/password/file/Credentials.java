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
package io.prestosql.plugin.password.file;

import java.util.Objects;

public class Credentials
{
    private final String user;
    private final String password;

    public Credentials(String username, String password)
    {
        this.user = username;
        this.password = password;
    }

    public String getInputUsername()
    {
        return this.user;
    }

    public String getInputPassword()
    {
        return this.password;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(user, password);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Credentials o = (Credentials) obj;
        return Objects.equals(user, o.getInputUsername()) &&
                Objects.equals(password, o.getInputPassword());
    }
}
