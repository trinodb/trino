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
package io.trino.server.security.oauth2;

import io.airlift.units.Duration;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static java.time.temporal.ChronoUnit.MILLIS;

public class TestingClock
        extends Clock
{
    private Instant currentTime = ZonedDateTime.of(2022, 5, 6, 10, 15, 0, 0, ZoneId.systemDefault()).toInstant();

    @Override
    public ZoneId getZone()
    {
        return ZoneId.systemDefault();
    }

    @Override
    public Clock withZone(ZoneId zone)
    {
        return this;
    }

    @Override
    public Instant instant()
    {
        return Instant.from(currentTime);
    }

    public void advanceBy(Duration currentTimeDelta)
    {
        this.currentTime = currentTime.plus(currentTimeDelta.toMillis(), MILLIS);
    }

    public void advanceBy(java.time.Duration currentTimeDelta)
    {
        this.currentTime = currentTime.plus(currentTimeDelta.toMillis(), MILLIS);
    }
}
