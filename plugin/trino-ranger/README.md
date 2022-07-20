# Trino-Ranger plugin

This plugin is designed to be build and run inside Trino.

It works with vanilla Apache Ranger, version 2.2.1 and up. You dont need to customize Ranger at all.

Here are the setup steps.


### Full Enterprise ready Apache Ranger setup

 * Install and startup Ranger
   * Quick and dirty setup for Ranger
     * git clone Ranger. I recommend using official tagged released. Here is an example:
       * git clone --recursive --branch release-ranger-2.2.0  https://github.com/apache/ranger
     * Build Ranger. Here is the commands I use
       * mvn -e -X clean package -pl '!plugin-kylin,!ranger-kylin-plugin-shim'  -DskipTests
     * You are looking for all of the build targets under "target/ranger-*.tar.gz"
     * Copy the main Ranger file to your binary install directory and expand it. Lets call this directory $RANGER_HOME
       * The main service to install is the ranger-$RANGER_VERSION-admin.tar.gz
     * Ranger has 2 configuration files that generate the full ranger install
       * $RANGER_HOME/install.properties
         * You need to generate this config. There is already a sample install.properties extracted from the admin.tar.gz file
           * Most of the defaults work fine. The configs you want to pay attention to are
             * PYTHON_COMMAND_INVOKER, DB_FLAVOR, SQL_CONNECTOR_JAR, db_root_user, db_root_password, db_host, db_name, db_user, db_password, rangerAdmin_password, rangerTagsync_password, rangerUsersync_password, keyadmin_password
       * $RANGER_HOME/usersync/install.properties
         * In most enterprise environments, ActiveDirectory/LDAP are used to manage users and their roles.
         * I do NOT recommend trying to manage users inside of Apache Ranger UI. I would configure the usersync service to pull the groups from AD/LDAP and attach your Ranger SQL policies to those external AD/LDAP groups.
         * Most of the defaults work fine. The configs you want to pay attention to are
           * SYNC_SOURCE=(ldap), rangerUsersync_password (configured above), SYNC_INTERVAL, SYNC_LDAP_URL, SYNC_LDAP_BIND_DN, SYNC_LDAP_BIND_PASSWORD, SYNC_LDAP_DELTASYNC, SYNC_LDAP_SEARCH_BASE, SYNC_LDAP_USER_SEARCH_BASE, SYNC_LDAP_USER_SEARCH_SCOPE, SYNC_LDAP_USER_OBJECT_CLASS, SYNC_LDAP_USER_SEARCH_FILTER, SYNC_LDAP_USER_NAME_ATTRIBUTE, SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE, SYNC_LDAP_USERNAME_CASE_CONVERSION, SYNC_LDAP_GROUPNAME_CASE_CONVERSION
           * SYNC_INTERVAL specifies the time in minutes that you want Ranger to synchronize AD/LDAP. This can take some time so dont make it too short. 360 (6 hours) is a good number.
     * With both the $RANGER_HOME/install.properties and $RANGER_HOME/usersync/install.properties configured run the setup scripts
       * $RANGER_HOME/setup.sh
       * $RANGER_HOME/usersync/setup.sh
     * Now try and start the main ranger service.
       * $RANGER_HOME/ews/start-ranger-admin.sh
         * I highly recommend monitoring the ranger PID, /run/ranger/rangeradmin.pid, and restarting it of it should fail. If using Docker/kubernetes a simple bash script like so works well
         ```
         ranger_admin_pid=`cat /run/ranger/rangeradmin.pid` > /dev/null 2>&1

         echo "${0##*/}:$LINENO: Waiting for ranger_admin_pid = $ranger_admin_pid"

         while s=`ps -p $ranger_admin_pid -o s=` && [[ "$s" && "$s" != 'Z' ]]; do
             sleep 1
         done

         echo "${0##*/}:$LINENO: Ranger admin service exited!!!"
         ```

     * Now try and start the AD/LDAP usersync serivce
       * $RANGER_HOME/usersync/ranger-usersync-services.sh start
       * NOTE: If you are running multiple instances of the Ranger UI, you should only ever have 1 and only 1 AD/LDAP usersync service running. Your groups will not properly sync otherwise.
     * Now login to the ranger UI and configure the Trino service.
       * Example:
         * http://localhost:6080
         * User: admin, Password: (defined above in rangerAdmin_password)
     * Create your Trino service.
       * The name is important and needs to be configured in the access-control-ranger.properties file. As of version 2.2.0, just use the Presto service type. Presto, also known as PrestoSQL, is the old name for Trino.
       * The defult policies that Ranger installs under your above service is WIDE open. Everything works for everyone. I will not document the full ranger setup.
       * Just remember, everything a JDBC driver sees, ranger also sees. This means even simple things like date_time functions will break in a fully locked down environment.       


### Connect Trino to Ranger

 * Connecting Trino to Ranger involved 5 files.
   * Main Trino config.properties
     * access-control.config-files=/usr/lib/trino/etc/access-control-ranger.properties
     * Ranger will lockdown EVERYTHING, no user can see another users queries. If you need a system wide user inside trino that can see the problems across the cluster you should add a access-control-file-based.properties to the above comma-separated list.
   * The access-control-ranger.properties file itself. Here is an example
   ```
   access-control.name=ranger
   ranger.use_ugi=true
   ranger.service_name=trino-dev-companyname-com
   ranger.hadoop_config=/workspace/testing/trino-server-dev/etc/trino-ranger-site.xml
   ranger.audit_resource=/workspace/testing/trino-server-dev/etc/ranger-trino-audit.xml
   ranger.security_resource=/workspace/testing/trino-server-dev/etc/ranger-trino-security.xml
   ranger.policy_manager_ssl_resource=/workspace/testing/trino-server-dev/etc/ranger-policymgr-ssl.xml
   ```
     * The ranger.service_name is the name of the service you created under the Ranger UI
   * ranger.hadoop_config=
     * Example file: plugins/trino-ranger/conf/trino-ranger-site.xml
     * Hopefully using hadoop is a temporary option. Hadoop is very heavy weight system just to load config files
     * This setup file, when a user logs in and executes the first SQL, it will pull the list of the users groups into Trino. This group list is used to match against the Ranger UI SQL policies you setup.
   * ranger.audit_resource
     * Example file: plugins/trino-ranger/conf/ranger-trino-site.xml
     * I dont recommend this but up to you. It was original Ranger features. As an alternative try the trino-http-event-listener and send every incoming SQL query to a kafka pipe. This way you can take the SQL contents and ingest into any system you want. Real time alerting is easier via using kafka pipes.
   * ranger.security_resource=
     * Example file: plugins/trino-ranger/conf/ranger-trino-security.xml
     * This is the main file that maps trino and ranger together. Only the 2 below are critical, the rest of the configurations the defaults are fine
       * ranger.plugin.trino.service.name is the same as the ranger.service_name entry.
       * ranger.plugin.trino.policy.rest.url is the URL of the Ranger admin service.
   * ranger.policy_manager_ssl_resource=
     * Example file: plugins/trino-ranger/conf/ranger-trino-security.xml
     * Defaults are fine. Java keystore is a pain but if you feel its necessary feel free to set it up.

