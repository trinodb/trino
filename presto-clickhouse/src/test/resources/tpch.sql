CREATE TABLE tpch.customer (custkey bigint,name  String,address  String,nationkey bigint,phone  String,acctbal double,mktsegment  String,comment  String) ENGINE=Log
CREATE TABLE tpch.lineitem (orderkey bigint,partkey bigint,suppkey bigint,linenumber integer,quantity double,extendedprice double,discount double,tax double,returnflag  String,linestatus  String,shipdate String,commitdate String,receiptdate String,shipinstruct  String,shipmode  String,comment  String) ENGINE=Log
CREATE TABLE tpch.nation (nationkey bigint, name String, regionkey bigint, comment String) ENGINE=Log
CREATE TABLE tpch.orders (orderkey bigint, custkey bigint, orderstatus  String, totalprice double, orderdate String, orderpriority  String, clerk String, shippriority integer, comment  String) ENGINE=Log
CREATE TABLE tpch.part (partkey bigint, name String, mfgr  String, brand  String, type  String, size integer, container String, retailprice double, comment String) ENGINE=Log
CREATE TABLE tpch.partsupp (partkey bigint,suppkey bigint,availqty integer,supplycost double,comment  String) ENGINE=Log
CREATE TABLE tpch.region (regionkey bigint,name  String,comment  String) ENGINE=Log
CREATE TABLE tpch.supplier (suppkey bigint,name  String,address  String, nationkey bigint, phone String, acctbal double, comment  String) ENGINE=Log
