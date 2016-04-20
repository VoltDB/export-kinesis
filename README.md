# VoltDB Kinesis Firehose Export Conduit

An experimental VoltDB to Kinesis Firehose export conduit [Kinesis Firehose API]
(http://docs.aws.amazon.com/firehose/latest/dev/what-is-this-service.html). It
allows export stream writers to push data directly into correspoding Kinesis Firehose streams.

{note:title=Be Careful}Keep in mind that Kinesis Firehose streams are relatively narrow when compared to VoltDB's
throughput (5,000 rows per second){note}

## How to build artifacts and setup Eclipse

* Install Gradle

On a Mac if you have [Homebrew](http://brew.sh/) setup then simply install the gradle bottle

```bash
brew install gradle
```

On Linux setup [GVM](http://gvmtool.net/), and install gradle as follows

```bash
gvm install gradle
```

* Create `gradle.properties` file and set the `voltdbhome` property
   to the base directory where your VoltDB is installed

```bash
echo voltdbhome=/voltdb/home/dirname > gradle.properties
```

* Invoke gradle to compile artifacts

```bash
gradle shadowJar
```

* To setup an eclipse project run gradle as follows

```bash
gradle cleanEclipse eclipse
```
then import it into your eclipse workspace by using File->Import projects menu option

## Configuration

* Copy the built jar from `build/libs` to `lib/extension` under your VoltDB installation directory

* Edit your deployment file and use the following export XML stanza as a template

```xml
<?xml version="1.0"?>
<deployment>
    <cluster hostcount="1" sitesperhost="4" kfactor="0" />
    <httpd enabled="true">
        <jsonapi enabled="true" />
    </httpd>
    <export>
        <configuration target="default" enabled="true" type="custom"
            exportconnectorclass="org.voltdb.exportclient.KinesisFirehoseExportClient">
            <property name="region">us-east-1</property>
            <property name="stream.name">streamtest</property>
            <property name="access.key"></property>
            <property name="secret.key"></property>
        </configuration>
    </export>
</deployment>
```

This tells VoltDB to write to the alerts stream and send the content to the Amazon Kinesis Firehose stream
with the name streamtest. If the client created with the supplied access.key and secret.key have access
to this stream then this stream will be successfully created. In this example we create the VoltDB export
with the definition:

```sql
CREATE STREAM alerts EXPORT TO TARGET default (
  id integer not null,
  msg varchar(128),
  continent varchar(64),
  country varchar(64)
);
```

Then data can be inserted into this export stream using the command:

```sql
INSERT INTO ALERTS (ID,MSG,CONTINENT,COUNTRY) VALUES (1,'fab-02 inoperable','EU','IT');
```

## Configuration Properties

- `region` (mandatory) designates the AWS region where the Kinesis Firehose stream is defined
- `stream.name`  (mandatory) Kinesis Firehose stream name
- `access.key` (mandatory) user's access key
- `secret.key` (mandatory) user's secret key
- `skipinternals` (optional, _default:_ false) flag to skip adding internal metadata to each row
- `timezone` (optional, _default:_ local timezone) timezone used to format timestamp values
- `binaryencoding` (optional, _default:_ hex) Specifies whether VARBINARY data is encoded in hexadecimal or BASE64 format.
- `type` (optional, _default:_ csv) specifies whether export format is csv or tsv.
