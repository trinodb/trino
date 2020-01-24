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
package io.prestosql.tests.product.launcher.env;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.prestosql.tests.product.launcher.env.common.Hadoop;
import io.prestosql.tests.product.launcher.env.common.Kerberos;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.env.environment.Multinode;
import io.prestosql.tests.product.launcher.env.environment.MultinodeTls;
import io.prestosql.tests.product.launcher.env.environment.MultinodeTlsKerberos;
import io.prestosql.tests.product.launcher.env.environment.Singlenode;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeCassandra;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeHdfsImpersonation;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeHiveImpersonation;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKafka;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosHdfsImpersonation;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosHdfsNoImpersonation;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosHiveImpersonation;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeMySql;
import io.prestosql.tests.product.launcher.env.environment.SinglenodePostgreSql;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeSqlServer;
import io.prestosql.tests.product.launcher.env.environment.TwoKerberosHives;
import io.prestosql.tests.product.launcher.env.environment.TwoMixedHives;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static java.util.Objects.requireNonNull;

public final class EnvironmentModule
        implements Module
{
    private final Module additionalEnvironments;

    public EnvironmentModule(Module additionalEnvironments)
    {
        this.additionalEnvironments = requireNonNull(additionalEnvironments, "additionalEnvironments is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(EnvironmentFactory.class);
        binder.bind(SelectedEnvironmentProvider.class);
        binder.bind(Standard.class);
        binder.bind(Hadoop.class);
        binder.bind(Kerberos.class);

        MapBinder<String, EnvironmentProvider> environments = newMapBinder(binder, String.class, EnvironmentProvider.class);

        environments.addBinding("singlenode").to(Singlenode.class);

        environments.addBinding("singlenode-hive-impersonation").to(SinglenodeHiveImpersonation.class);
        environments.addBinding("singlenode-kerberos-hive-impersonation").to(SinglenodeKerberosHiveImpersonation.class);

        environments.addBinding("singlenode-hdfs-impersonation").to(SinglenodeHdfsImpersonation.class);
        environments.addBinding("singlenode-kerberos-hdfs-impersonation").to(SinglenodeKerberosHdfsImpersonation.class);
        environments.addBinding("singlenode-kerberos-hdfs-no-impersonation").to(SinglenodeKerberosHdfsNoImpersonation.class);

        environments.addBinding("multinode").to(Multinode.class);
        environments.addBinding("multinode-tls").to(MultinodeTls.class);
        environments.addBinding("multinode-tls-kerberos").to(MultinodeTlsKerberos.class);

        environments.addBinding("two-kerberos-hives").to(TwoKerberosHives.class);
        environments.addBinding("two-mixed-hives").to(TwoMixedHives.class);

        environments.addBinding("singlenode-cassandra").to(SinglenodeCassandra.class);
        environments.addBinding("singlenode-kafka").to(SinglenodeKafka.class);
        environments.addBinding("singlenode-mysql").to(SinglenodeMySql.class);
        environments.addBinding("singlenode-postgresql").to(SinglenodePostgreSql.class);
        environments.addBinding("singlenode-sqlserver").to(SinglenodeSqlServer.class);

        binder.install(additionalEnvironments);
    }
}
