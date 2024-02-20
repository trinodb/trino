# Glossary

The glossary contains a list of key Trino terms and definitions.

(glosscatalog)=

Catalog

: Catalogs define and name a configuration for connecting to a data source,
  allowing users to query the connected data. Each catalog's configuration
  specifies a {ref}`connector <glossConnector>` to define which data source
  the catalog connects to. For more information about catalogs, see
  {ref}`trino-concept-catalog`.

(glosscert)=

Certificate

: A public key [certificate](https://wikipedia.org/wiki/Public_key_certificate) issued by a {ref}`CA
  <glossCA>`, sometimes abbreviated as cert, that verifies the ownership of a
  server's private keys. Certificate format is specified in the [X.509](https://wikipedia.org/wiki/X.509) standard.

(glossca)=

Certificate Authority (CA)

: A trusted organization that signs and issues certificates. Its signatures
  can be used to verify the validity of {ref}`certificates <glossCert>`.

Cluster

: A Trino cluster provides the resources to run queries against numerous data
  sources. Clusters define the number of nodes, the configuration for the JVM
  runtime, configured data sources, and others aspects. For more information,
  see {ref}`trino-concept-cluster`.

(glossconnector)=

Connector

: Translates data from a data source into Trino schemas, tables, columns,
  rows, and data types. A {doc}`connector </connector>` is specific to a data
  source, and is used in {ref}`catalog <glossCatalog>` configurations to
  define what data source the catalog connects to. A connector is one of many
  types of {ref}`plugins <glossPlugin>`

Container

: A lightweight virtual package of software that contains libraries, binaries,
  code, configuration files, and other dependencies needed to deploy an
  application. A running container does not include an operating system,
  instead using the operating system of the host machine. To learn more, read
  read about [containers](https://kubernetes.io/docs/concepts/containers/)
  in the Kubernetes documentation.

(glossdatasource)=

Data source

: A system from which data is retrieved - for example, PostgreSQL or Iceberg
  on S3 data. In Trino, users query data sources with {ref}`catalogs
  <glossCatalog>` that connect to each source. See
  {ref}`trino-concept-data-sources` for more information.

(glossdatavirtualization)=

Data virtualization

: [Data virtualization](https://wikipedia.org/wiki/Data_virtualization) is a
  method of abstracting an interaction with multiple {ref}`heterogeneous data
  sources <glossDataSource>`, without needing to know the distributed nature
  of the data, its format, or any other technical details involved in
  presenting the data.

(glossgzip)=

gzip

: [gzip](https://wikipedia.org/wiki/Gzip) is a compression format and
  software that compresses and decompresses files. This format is used several
  ways in Trino, including deployment and compressing files in {ref}`object
  storage <glossObjectStorage>`. The most common extension for gzip-compressed
  files is `.gz`.

(glosshdfs)=

HDFS

: [Hadoop Distributed Filesystem (HDFS)](https://wikipedia.org/wiki/Apache_Hadoop#HDFS) is a scalable {ref}`open
  source <glossOpenSource>` filesystem that was one of the earliest
  distributed big data systems created to store large amounts of data for the
  [Hadoop ecosystem](https://wikipedia.org/wiki/Apache_Hadoop).

(glossjks)=

Java KeyStore (JKS)

: The system of public key cryptography supported as one part of the Java
  security APIs. The legacy JKS system recognizes keys and {ref}`certificates
  <glossCert>` stored in *keystore* files, typically with the `.jks`
  extension, and by default relies on a system-level list of {ref}`CAs
  <glossCA>` in *truststore* files installed as part of the current Java
  installation.

Key

: A cryptographic key specified as a pair of public and private strings
  generally used in the context of {ref}`TLS <glossTLS>` to secure public
  network traffic.

(glosslb)=

Load Balancer (LB)

: Software or a hardware device that sits on a network edge and accepts
  network connections on behalf of servers behind that wall, distributing
  traffic across network and server infrastructure to balance the load on
  networked services.

(glossobjectstorage)=

Object storage

: [Object storage](https://en.wikipedia.org/wiki/Object_storage) is a file
  storage mechanism. Examples of compatible object stores include the
  following:

  - [Amazon S3](https://aws.amazon.com/s3)
  - [Google Cloud Storage](https://cloud.google.com/storage)
  - [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs)
  - [MinIO](https://min.io/) and other S3-compatible stores
  - {ref}`HDFS <glossHDFS>`

(glossopensource)=

Open-source

: Typically refers to [open-source software](https://wikipedia.org/wiki/Open-source_software). which is software that
  has the source code made available for others to see, use, and contribute
  to. Allowed usage varies depending on the license that the software is
  licensed under. Trino is licensed under the [Apache license](https://wikipedia.org/wiki/Apache_License), and is therefore maintained
  by a community of contributors from all across the globe.

(glosspem)=

PEM file format

: A format for storing and sending cryptographic keys and certificates. PEM
  format can contain both a key and its certificate, plus the chain of
  certificates from authorities back to the root {ref}`CA <glossCA>`, or back
  to a CA vendor's intermediate CA.

(glosspkcs12)=

PKCS #12

: A binary archive used to store keys and certificates or certificate chains
  that validate a key. [PKCS #12](https://wikipedia.org/wiki/PKCS_12) files
  have `.p12` or `.pfx` extensions. This format is a less popular
  alternative to {ref}`PEM <glossPEM>`.

(glossplugin)=

Plugin

: A bundle of code implementing the Trino {doc}`Service Provider Interface
  (SPI) </develop/spi-overview>` that is used to add new {ref}`connectors
  <glossConnector>`, {doc}`data types </develop/types>`, {doc}`functions`,
  {doc}`access control implementations </develop/system-access-control>`, and
  other features of Trino.

Presto and PrestoSQL

: The old name for Trino. To learn more about the name change to Trino, read
  [the history](<https://wikipedia.org/wiki/Trino_(SQL_query_engine)#History>).

Query federation

: A type of {ref}`data virtualization <glossDataVirtualization>` that provides a
  common access point and data model across two or more heterogeneous data
  sources. A popular data model used by many query federation engines is
  translating different data sources to {ref}`SQL <glossSQL>` tables.

(glossssl)=

Secure Sockets Layer (SSL)

: Now superseded by {ref}`TLS <glossTLS>`, but still recognized as the term
  for what TLS does.

(glosssql)=

Structured Query Language (SQL)

: The standard language used with relational databases. For more information,
  see {doc}`SQL </language>`.

Tarball

: A common abbreviation for [TAR file](<https://wikipedia.org/wiki/Tar_(computing)>), which is a common software
  distribution mechanism. This file format is a collection of multiple files
  distributed as a single file, commonly compressed using {ref}`gzip
  <glossGzip>` compression.

(glosstls)=

Transport Layer Security (TLS)

: [TLS](https://wikipedia.org/wiki/Transport_Layer_Security) is a security
  protocol designed to provide secure communications over a network. It is the
  successor to {ref}`SSL <glossSSL>`, and used in many applications like
  HTTPS, email, and Trino. These security topics use the term TLS to refer to
  both TLS and SSL.
