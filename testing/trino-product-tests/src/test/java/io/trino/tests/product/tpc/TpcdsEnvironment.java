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
package io.trino.tests.product.tpc;

import java.util.List;
import java.util.Map;

public class TpcdsEnvironment
        extends AbstractTpcEnvironment
{
    @Override
    protected String sourceCatalog()
    {
        return "tpcds";
    }

    @Override
    protected Map<String, String> sourceCatalogProperties()
    {
        return Map.of("connector.name", "tpcds");
    }

    @Override
    protected String targetSchema()
    {
        return "tpcds";
    }

    @Override
    protected List<String> tables()
    {
        return List.of(
                "call_center",
                "catalog_page",
                "catalog_returns",
                "catalog_sales",
                "customer",
                "customer_address",
                "customer_demographics",
                "date_dim",
                "household_demographics",
                "income_band",
                "inventory",
                "item",
                "promotion",
                "reason",
                "ship_mode",
                "store",
                "store_returns",
                "store_sales",
                "time_dim",
                "warehouse",
                "web_page",
                "web_returns",
                "web_sales",
                "web_site");
    }
}
