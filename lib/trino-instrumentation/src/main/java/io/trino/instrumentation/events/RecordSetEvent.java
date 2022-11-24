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
package io.trino.instrumentation.events;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("io.trino.spi.connector.RecordSet")
@Label("RecordSet")
@Description("Traces RecordSet SPI invocations")
@Category({"Trino SPI", "Connector"})
public class RecordSetEvent
        extends Event
{
    @Label("Class")
    @Description("Concrete implementation that was called")
    public String className;

    @Label("Method")
    @Description("SPI method that was called")
    public String method;

    @Label("Catalog Name")
    @Description("Catalog name to which event invocation belongs (if available)")
    public String catalogName;

    @Label("Query Id")
    @Description("Query ID to which event invocation belongs (if available)")
    public String queryId;
}
