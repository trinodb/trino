===================
General FAQ
===================

This frequently-asked-questions list answers some of your more common questions about how to configure and use Trino.

Why is Java 11 needed to run Trino?
-----------------------------------

One thing to point out is you’re only required to use JDK11 for the server. The client can be on JRE 8. One reason you would need to run Trino on JRE 8 is if the server had to be run with another service running JRE 8 which we do not recommend as this will degrade the performance of your cluster and could cause other issues if Trino is fighting for resources. Even so you could technically install both versions of Java on the same machine if you are running in a testing environment or doing a proof of concept but we don't recommend this set up for production use if at all.

Another possibility is that there is a company policy requiring specific JDKs be installed on all servers. You can have side-by-side installs of multiple versions of the JDK and use the appropriate one. You just need to launch Presto with the correct java command. If your company is against using a newer JDK, you can point out the arguments above to update the policy to at least include JDK11.

Can I still run on JRE 8 if there's no other way?
-------------------------------------------------

You cannot run the latest version of Trino using JRE 8. `Release 330 <https://trino.io/docs/current/release/release-330.html#server-changes>`_ was the initial version that defaulted to running on JRE 11 but left a fallback option by adding ``-Dpresto-temporarily-allow-java8=true`` to the Trino JVM Config. The last version that supports using this fallback and running the server on JRE 8 is version 332 (see `release 333 <https://trino.io/docs/current/release/release-333.html#server-changes>`_). 

It should be noted that `release 337 <https://trino.io/docs/current/release/release-337.html#security-changes>`_ fixes `a critical CVE <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-15087>`_ and it is not recommended to use versions before this and yet another reason we do not advise using any prior versions which include all versions that could run on JRE 8.

Aside from the security and performance issues, and the fallback you have to enable, these versions also use the ``io.prestosql`` namespace rather than the ``io.trino`` namespace along with a few other changes. This `would require a migration <https://trino.io/blog/2021/01/04/migrating-from-prestosql-to-trino.html>`_ when you decide to update to the later versions. 

If you still wish to proceed, you can either `download from maven <https://repo.maven.apache.org/maven2/io/prestosql/presto-server/332/>`_ or an older `Trino (formerly PrestoSQL) document version <https://trino.io/docs/332/>`_. Both links will take you to version 332 but you can specify any that you like. 

Will adding new worker nodes to a running query utilize the resources of the new nodes?
---------------------------------------------------------------------------------------

New workers will only be utilized for table scans, if and only if the splits are not already assigned. The machines used for hash partitions (i.e. aggregation and joins), are fixed when the query starts.

There's no way to know the exact memory requirements without executing the query, since you don't know how much will be filtered, or how much data is produced by joins and other operations. Per-node memory requirements are also dependent on skew, where the global memory requirement is easier. 



Do I need to worry about reserved memory?
----------------------------------------------------------------------
Todo: I think we need to clean up how we discuss setting memory properties first before talking about reserved memories.
Then maybe link Dain's discussion on optimal memory settings or somthing.


