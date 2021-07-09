# AWS S3 Connector Yaml Profile Loading

This connector supports loading connector configuration via Yaml.

## Specifying Profiles

To specify a profile in your connector config you can use the following setting in your main connector configuration:

    connect.s3.config.profiles=/path/to/yaml/profile1.yaml,/path/to/yaml/profile2.yaml

Multiple configuration profiles can be specified via connector configuration.

They will be loaded and the KCQL configurations will be merged.


## Yaml File Format

Here is an example Yaml configuration file.  Any of the properties as detailed in the documentation can be integrated into the Yaml file.

    ---
    connect.s3.aws.access.key: myAccessKey
    connect.s3.aws.secret.key: mySecretKey
    connect.s3.aws.auth.mode: myAuthMode
    connect.s3.kcql: insert into `target-bucket:target-path` select * from `source.bucket` PARTITIONBY name,title,salary STOREAS `text` WITH_FLUSH_SIZE = 500000000 WITH_FLUSH_INTERVAL = 3600 WITH_FLUSH_COUNT = 50000

For more flexibility when working with KCQL, a kcql builder Yaml map can be supplied in exchange for the Kcql String.

    ---
    connect.s3.kcql.builder:
      source: my-kafka-topic
      target: myBucket:myPartition
      format: csv
      partitions: name,title,salary
      partitioner: Values
      flush_size: 1
      flush_interval: 2
      flush_count: 3

This will be used by the connector to build the relevant KCQL string at startup.

## KCQL in connector configuration

Due to limitations, if supplying the KCQL or the KCQL builder yaml, the source and target are always mandatory.

To avoid having to set them use the special keyword "USE_PROFILE", which will ignore this property.

For example a minimal kcql string:

    connect.s3.kcql=insert into USE_PROFILE select * from USE_PROFILE

Using this will enable you to set a mandatory KCQL string however fall back on the values in the profiles.

Of course, the connector will fail if the valid options are not set somewhere in the profile chain. 
