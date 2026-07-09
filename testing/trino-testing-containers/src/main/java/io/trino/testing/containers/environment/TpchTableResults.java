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
package io.trino.testing.containers.environment;

import java.util.List;

import static io.trino.testing.containers.environment.Row.row;

/**
 * Expected results from TPCH tables for use in product test assertions.
 * <p>
 * These constants replace the tempto SqlResultDescriptor pattern with simple Java constants.
 * The data matches the TPCH specification and Trino's tpch connector output.
 */
public final class TpchTableResults
{
    private TpchTableResults() {}

    /**
     * The number of rows in the nation table.
     */
    public static final int NATION_ROW_COUNT = 25;

    /**
     * All 25 nation rows from the TPCH nation table.
     * <p>
     * Schema: nationkey (bigint), name (varchar), regionkey (bigint), comment (varchar)
     * <p>
     * Ordered by nationkey (0-24).
     */
    public static final List<Row> NATION_ROWS = List.of(
            row(0L, "ALGERIA", 0L, " haggle. carefully final deposits detect slyly agai"),
            row(1L, "ARGENTINA", 1L, "al foxes promise slyly according to the regular accounts. bold requests alon"),
            row(2L, "BRAZIL", 1L, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special "),
            row(3L, "CANADA", 1L, "eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold"),
            row(4L, "EGYPT", 4L, "y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d"),
            row(5L, "ETHIOPIA", 0L, "ven packages wake quickly. regu"),
            row(6L, "FRANCE", 3L, "refully final requests. regular, ironi"),
            row(7L, "GERMANY", 3L, "l platelets. regular accounts x-ray: unusual, regular acco"),
            row(8L, "INDIA", 2L, "ss excuses cajole slyly across the packages. deposits print aroun"),
            row(9L, "INDONESIA", 2L, " slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull"),
            row(10L, "IRAN", 4L, "efully alongside of the slyly final dependencies. "),
            row(11L, "IRAQ", 4L, "nic deposits boost atop the quickly final requests? quickly regula"),
            row(12L, "JAPAN", 2L, "ously. final, express gifts cajole a"),
            row(13L, "JORDAN", 4L, "ic deposits are blithely about the carefully regular pa"),
            row(14L, "KENYA", 0L, " pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t"),
            row(15L, "MOROCCO", 0L, "rns. blithely bold courts among the closely regular packages use furiously bold platelets?"),
            row(16L, "MOZAMBIQUE", 0L, "s. ironic, unusual asymptotes wake blithely r"),
            row(17L, "PERU", 1L, "platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun"),
            row(18L, "CHINA", 2L, "c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos"),
            row(19L, "ROMANIA", 3L, "ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account"),
            row(20L, "SAUDI ARABIA", 4L, "ts. silent requests haggle. closely express packages sleep across the blithely"),
            row(21L, "VIETNAM", 2L, "hely enticingly express accounts. even, final "),
            row(22L, "RUSSIA", 3L, " requests against the platelets use never according to the quickly regular pint"),
            row(23L, "UNITED KINGDOM", 3L, "eans boost carefully special requests. accounts are. carefull"),
            row(24L, "UNITED STATES", 1L, "y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be"));

    /**
     * The number of rows in the region table.
     */
    public static final int REGION_ROW_COUNT = 5;

    /**
     * All 5 region rows from the TPCH region table.
     * <p>
     * Schema: regionkey (bigint), name (varchar), comment (varchar)
     */
    public static final List<Row> REGION_ROWS = List.of(
            row(0L, "AFRICA", "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to "),
            row(1L, "AMERICA", "hs use ironic, even requests. s"),
            row(2L, "ASIA", "ges. thinly even pinto beans ca"),
            row(3L, "EUROPE", "ly final courts cajole furiously final excuse"),
            row(4L, "MIDDLE EAST", "uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl"));
}
