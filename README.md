## Requirements

- sbt
- spark 2.3
- java 6+

## Run locally with spark

- `sbt assembly`
- `spark-submit --master "local[*]" target/scala-2.11/spark-windows.jar`

## Remarks

- Change configuration _application.conf_ in order to run against some other dataset with same scheme 