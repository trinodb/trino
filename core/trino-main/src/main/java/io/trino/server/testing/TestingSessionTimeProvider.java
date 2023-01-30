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
package io.trino.server.testing;

import io.trino.Session;
import io.trino.sql.analyzer.SessionTimeProvider;

import java.time.Instant;

import static io.trino.server.testing.TestingSystemSessionProperties.TESTING_SESSION_TIME;

public class TestingSessionTimeProvider
        implements SessionTimeProvider
{
    @Override
    public Instant getStart(Session session)
    {
        String testingTime = session.getSystemProperty(TESTING_SESSION_TIME, String.class);
        if (testingTime != null) {
            return Instant.parse(testingTime);
        }
        return session.getStart();
    }
}
