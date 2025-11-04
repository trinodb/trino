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
package io.trino.plugin.teradata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.plugin.jdbc.BaseJdbcConfig;

public class TeradataConfig
        extends BaseJdbcConfig
{
    private String logMech = "TD2";
    private TeradataCaseSensitivity teradataCaseSensitivity = TeradataCaseSensitivity.CASE_SENSITIVE;

    public String getLogMech()
    {
        return logMech;
    }

    @Config("logon-mechanism")
    @ConfigDescription("Specifies the logon mechanism for Teradata (default: TD2). Use 'TD2' for TD2 authentication.")
    public TeradataConfig setLogMech(String logMech)
    {
        this.logMech = logMech;
        return this;
    }

    public TeradataCaseSensitivity getTeradataCaseSensitivity()
    {
        return teradataCaseSensitivity;
    }

    @Config("teradata.case-sensitivity")
    @ConfigDescription("How char/varchar columns' case sensitivity will be exposed to Trino (default: CASE_SENSITIVE). Possible values: CASE_INSENSITIVE, CASE_SENSITIVE, AS_DEFINED.")
    public TeradataConfig setTeradataCaseSensitivity(TeradataCaseSensitivity teradataCaseSensitivity)
    {
        this.teradataCaseSensitivity = teradataCaseSensitivity;
        return this;
    }

    public enum TeradataCaseSensitivity
    {
        CASE_INSENSITIVE, CASE_SENSITIVE, AS_DEFINED
    }
}
