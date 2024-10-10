# DataSpawner

TODO.

## Get Started

.

### Prerequisites

```shell
uv venv
source venv/bin/activate
uv sync
```

### Build

```shell
uv build
```

## Supported PySpark Data Types

A list of all the PySpark data types can be found [here](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html).

| Data Types              | Status               | Comment                                                                                                                                         |
|-------------------------|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `ArrayType`             | :white_large_square: |                                                                                                                                                 |
| `BinaryType`            | :white_large_square: |                                                                                                                                                 |
| `BooleanType`           | :white_check_mark:   |                                                                                                                                                 |
| `ByteType`              | :white_large_square: |                                                                                                                                                 |
| `DataType`              | :white_large_square: | Not supported by data contract.                                                                                                                 |
| `DateType`              | :white_large_square: |                                                                                                                                                 |
| `DecimalType`           | :white_check_mark:   | Requires that `precision` and `scale` is defined. A [PR](https://github.com/datacontract/datacontract-cli/pull/450) exists to handle this case. |
| `DoubleType`            | :white_check_mark:   |                                                                                                                                                 |
| `FloatType`             | :white_check_mark:   |                                                                                                                                                 |
| `IntegerType`           | :white_check_mark:   |                                                                                                                                                 |
| `LongType`              | :white_check_mark:   |                                                                                                                                                 |
| `MapType`               | :white_large_square: |                                                                                                                                                 |
| `NullType`              | :white_check_mark:   |                                                                                                                                                 |
| `ShortType`             | :white_large_square: | Not supported by data contract.                                                                                                                 |
| `StringType`            | :white_check_mark:   |                                                                                                                                                 |
| `CharType`              | :white_large_square: | Not supported by data contract.                                                                                                                 |
| `VarcharType`           | :white_large_square: | Not supported by data contract.                                                                                                                 |
| `StructField`           | :white_large_square: | Not supported by data contract.                                                                                                                 |
| `TimestampType`         | :white_large_square: |                                                                                                                                                 |
| `TimestampNTZType`      | :white_large_square: |                                                                                                                                                 |
| `DayTimeIntervalType`   | :white_large_square: |                                                                                                                                                 |
| `YearMonthIntervalType` | :white_large_square: |                                                                                                                                                 |
