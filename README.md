# io

Sample codes for reading from, or writing to various data sources.

## Samples

|IO|Read/Write|Class path|
|:--|:--|:--|
|TextIO|Read|com.examples.beam.reader.TextIOReader|
||Write|com.examples.beam.writer.TextIOWriter|

## Execute

```bash
$ mvn -q compile exec:java \
     -D exec.mainClass=path.to.class \
     -D exec.args="--optionName=value ..."
```
