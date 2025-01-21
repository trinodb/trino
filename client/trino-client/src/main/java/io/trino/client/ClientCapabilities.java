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
package io.trino.client;

public enum ClientCapabilities
{
    PATH,
    // Whether clients support datetime types with variable precision
    //   timestamp(p) with time zone
    //   timestamp(p) without time zone
    //   time(p) with time zone
    //   time(p) without time zone
    //   interval X(p1) to Y(p2)
    // When this capability is not set, the server returns datetime types with precision = 3
    PARAMETRIC_DATETIME,
    // Whether clients support the session authorization set/reset feature
    SESSION_AUTHORIZATION,
    USE_SESSION_TIME_ZONE,
}
