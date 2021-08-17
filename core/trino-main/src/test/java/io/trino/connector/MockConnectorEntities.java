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
package io.trino.connector;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public final class MockConnectorEntities
{
    private MockConnectorEntities() {}

    public static final List<ColumnMetadata> TPCH_NATION_SCHEMA = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("nationkey", BIGINT))
            .add(new ColumnMetadata("name", createUnboundedVarcharType()))
            .add(new ColumnMetadata("regionkey", BIGINT))
            .add(new ColumnMetadata("comment", createUnboundedVarcharType()))
            .build();

    public static final List<List<?>> TPCH_NATION_DATA = ImmutableList.<List<?>>builder()
            .add(ImmutableList.of(0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai"))
            .add(ImmutableList.of(1, "ARGENTINA", 1, "al foxes promise slyly according to the regular accounts. bold requests alon"))
            .add(ImmutableList.of(2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special "))
            .add(ImmutableList.of(3, "CANADA", 1, "eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold"))
            .add(ImmutableList.of(4, "EGYPT", 4, "y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d"))
            .add(ImmutableList.of(5, "ETHIOPIA", 0, "ven packages wake quickly. regu"))
            .add(ImmutableList.of(6, "FRANCE", 3, "refully final requests. regular, ironi"))
            .add(ImmutableList.of(7, "GERMANY", 3, "l platelets. regular accounts x-ray: unusual, regular acco"))
            .add(ImmutableList.of(8, "INDIA", 2, "ss excuses cajole slyly across the packages. deposits print aroun"))
            .add(ImmutableList.of(9, "INDONESIA", 2, " slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull"))
            .add(ImmutableList.of(10, "IRAN", 4, "efully alongside of the slyly final dependencies. "))
            .add(ImmutableList.of(11, "IRAQ", 4, "nic deposits boost atop the quickly final requests? quickly regula"))
            .add(ImmutableList.of(12, "JAPAN", 2, "ously. final, express gifts cajole a"))
            .add(ImmutableList.of(13, "JORDAN", 4, "ic deposits are blithely about the carefully regular pa"))
            .add(ImmutableList.of(14, "KENYA", 0, " pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t"))
            .add(ImmutableList.of(15, "MOROCCO", 0, "rns. blithely bold courts among the closely regular packages use furiously bold platelets?"))
            .add(ImmutableList.of(16, "MOZAMBIQUE", 0, "s. ironic, unusual asymptotes wake blithely r"))
            .add(ImmutableList.of(17, "PERU", 1, "platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun"))
            .add(ImmutableList.of(18, "CHINA", 2, "c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos"))
            .add(ImmutableList.of(19, "ROMANIA", 3, "ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account"))
            .add(ImmutableList.of(20, "SAUDI ARABIA", 4, "ts. silent requests haggle. closely express packages sleep across the blithely"))
            .add(ImmutableList.of(21, "VIETNAM", 2, "hely enticingly express accounts. even, final "))
            .add(ImmutableList.of(22, "RUSSIA", 3, " requests against the platelets use never according to the quickly regular pint"))
            .add(ImmutableList.of(23, "UNITED KINGDOM", 3, "eans boost carefully special requests. accounts are. carefull"))
            .add(ImmutableList.of(24, "UNITED STATES", 1, "y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be"))
            .build();
}
