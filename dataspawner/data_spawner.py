"""The main DataSpawner module.

This module provides functionalities to generate and manage data for
testing and development purposes. It includes various data spawner
functions that can create sample datasets based on specified parameters.
"""

import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, getcontext
from typing import Any

import numpy as np
from datacontract.data_contract import DataContract
from datacontract.export.spark_converter import to_spark_schema
from datacontract.model.data_contract_specification import Field
from faker import Faker
from pyspark.sql import DataFrame, SparkSession, types
from rstr import xeger


def get_min_max(
    field: Field,
) -> tuple[int | float, int | float]:
    """Get the minimum and maximum values for a specified field.

    This method retrieves the minimum and maximum values from the
    dataset for the specified field. It allows for optional minimum
    and maximum value constraints, ensuring that the returned values
    respect these boundaries.

    Args:
        field (Field): The field for which the minimum and maximum
            values are to be fetched. This should be an instance of
            the Field class that represents a valid data column.

    Returns:
        tuple[int | float, int | float]: A tuple containing the
            adjusted minimum and maximum values for the specified
            field, respecting the provided boundaries.
    """
    min_value, max_value = 1e-10, 1e10
    if field.type == "int" or field.type == "integer":  # 32-bit signed integer
        min_value = -2 ** 31
        max_value = 2 ** 31 - 1
    elif field.type == "long":  # 64-bit signed integer
        min_value = -2 ** 63
        max_value = 2 ** 63 - 1
    elif field.type == "float":  # 32-bit single precision float
        min_value = float(np.finfo(np.float32).tiny / 1e+10)
        max_value = float(np.finfo(np.float32).max / 1e+10)
    elif field.type == "double":  # 64-bit double precision float
        min_value = float(np.finfo(np.float64).tiny / 1e+10)
        max_value = float(np.finfo(np.float64).max / 1e+10)

    minimum = field.minimum or min_value
    maximum = field.maximum or max_value
    exclusive_minimum = field.exclusiveMinimum or min_value
    exclusive_maximum = field.exclusiveMaximum or max_value

    minimum_value = max(minimum, exclusive_minimum)
    maximum_value = min(maximum, exclusive_maximum)

    if minimum_value <= 0 and field.minimum == 0 or field.exclusiveMinimum == 0:
        minimum_value = 0

    if maximum_value >= 0 and field.maximum == 0 or field.exclusiveMaximum == 0:
        maximum_value = 0

    return minimum_value, maximum_value


def generate_unsigned_value(
    field: Field,
) -> float:
    """Generate a synthetic uniform float value for a specified field.

    This method generates a random float value uniformly distributed
    within the range defined by the minimum and maximum constraints of
    the given field. The generated value is rounded to the specified
    number of decimal places.

    Args:
        field (Field): The field for which the uniform float value is
            to be generated. This should be an instance of the Field
            class that outlines the criteria for generating the float.

    Returns:
        float: A randomly generated float value within the specified
            range for the field, rounded to the specified number of
            decimal places.
    """
    minimum_value, maximum_value = get_min_max(field)
    decimal_places = field.scale if field.scale else 0

    return round(random.uniform(minimum_value, maximum_value), decimal_places)


def generate_decimal_value(
        field: Field,
) -> Decimal:
    """Generate a synthetic decimal value for a specified field.

    This method generates a random decimal value based on the
    precision and scale defined for the provided field. It ensures
    that the generated decimal adheres to the specified precision
    and scale constraints, raising errors for invalid inputs.

    Args:
        field (Field): The field for which the decimal value is to be
            generated. This should be an instance of the Field class
            that specifies the precision and scale for the generated
            decimal.

    Raises:
        ValueError: If the precision or scale is not an integer, if
            precision exceeds 38, or if the scale exceeds the
            specified precision.

    Returns:
        Decimal: A randomly generated decimal value formatted according
            to the specified precision and scale.
    """
    precision = field.precision if field.precision is not None else 10
    scale = field.scale if field.scale is not None else 0

    if not isinstance(precision, int) or not isinstance(scale, int):
        raise ValueError("Precision and scale must be integers.")

    if precision > 38:
        raise ValueError("Precision must be less than 38.")
    if precision < scale:
        raise ValueError("The scale must be less or equal to precision.")

    getcontext().prec = precision

    int_part = random.randint(0, 10 ** (precision - scale) - 1)

    if scale > 0:
        frac_part = random.randint(0, 10**scale - 1)
        decimal_str = f"{int_part}.{frac_part:0{scale}d}"
    else:
        decimal_str = f"{int_part}"

    return Decimal(decimal_str)


def generate_signed_value(
        field: Field,
) -> int:
    """Generate a synthetic signed value for a specified field.

    This method generates a random integer value based on the minimum
    and maximum constraints defined for the given field. It retrieves
    the constraints using the `get_min_max` function and ensures the
    generated value falls within this range.

    Args:
        field (Field): The field for which the integer value is to be
            generated. This should be an instance of the Field class
            that specifies the criteria for generating the integer.

    Returns:
        int: A randomly generated integer value within the specified
            range for the field. The value will be inclusive of the
            minimum and maximum bounds defined for the field.
    """
    minimum_value, maximum_value = get_min_max(field)

    return random.randint(int(minimum_value), int(maximum_value))


class DataSpawner:
    """A class to generate synthetic data for testing and development.

    This class provides methods to create various types of datasets,
    including random numerical data, categorical data, and time-series
    data. It can be configured with different parameters to customize
    the generated data.
    """

    def __init__(
        self,
        data_contract: DataContract,
        model_name: str,
        custom_faker_providers: list[Any] | None = None,
        faker_attribute_field: str = "faker",
        locales: list[str] | None = None,
        seed: int = 42,
        max_workers: int = 4,
        null_rate: float = 0.1,
    ):
        """Initialize the DataSpawner with the specified parameters.

        Args:
            data_contract (DataContract): An instance of the DataContract
                class that defines the schema and rules for the data
                generation.
            model_name (str): The name of the model to be used for data
                generation.
            custom_faker_providers (list[Any] | None, optional): A list of
                custom Faker providers to extend the data generation
                capabilities. Defaults to None.
            faker_attribute_field (str, optional): The field in the
                Faker library to be used for data generation. Defaults
                to "faker".
            locales (list[str] | None, optional): A list of locale
                strings to be used by Faker for generating localized data.
                Defaults to None.
            seed (int, optional): A seed value for random number
                generation to ensure reproducibility. Defaults to 42.
            max_workers (int, optional): The maximum number of worker
                threads to be used for concurrent data generation.
                Defaults to 4.
            null_rate (float, optional): The proportion of generated data
                points that should be null values. Should be between 0 and 1.
                Defaults to 0.1.
        """
        if custom_faker_providers is None:
            custom_faker_providers = []
        if locales is None:
            locales = ["en_US"]

        self.data_contract = data_contract
        self.model_name = model_name
        self.custom_faker_providers = custom_faker_providers
        self.faker_attribute_field = faker_attribute_field
        self.locales = locales
        self.seed = seed
        self.max_workers = max_workers
        self.null_rate = null_rate

        self.faker = Faker(locale=self.locales)
        self._set_seed(self.seed)
        self._set_data_contract_model(self.data_contract, self.model_name)
        self._set_custom_faker_providers(self.custom_faker_providers)

    def _set_seed(
        self,
        seed: int,
    ) -> None:
        random.seed(seed)
        Faker.seed(seed)

    def _set_data_contract_model(
        self,
        data_contract: DataContract,
        model_name: str,
    ) -> None:
        self.model = data_contract.get_data_contract_specification().models.get(
            model_name
        )
        self.schema = to_spark_schema(self.model)

    def _set_custom_faker_providers(
        self,
        custom_faker_providers: list[Any],
    ) -> None:
        for faker_provider in custom_faker_providers:
            self.faker.add_provider(faker_provider)

    def generate_faker_value(
        self,
        faker_attribute: str,
    ) -> Any:
        """Generate synthetic values using the Faker library.

        This method utilizes the specified Faker attribute to create one or
        more synthetic data points. It can be customized to return a list
        of values based on the number of samples requested.

        Args:
            faker_attribute (str): The attribute of the Faker library to be
                used for generating the synthetic data (e.g., 'name',
                'address', etc.).

        Returns:
            Any: A generated synthetic Faker value.
        """
        if faker_attribute == "uuid":
            faker_attribute = "uuid4"

        if faker_attribute is not None and hasattr(self.faker, faker_attribute):
            return getattr(self.faker, faker_attribute)()

        return None

    def generate_string_value(self, field: Field) -> str:
        """Generate a synthetic string value.

        This method creates a random or predefined string value based on the
        characteristics defined for the provided field. It may utilize any
        applicable rules, formats, or patterns associated with the field to
        ensure that the generated string is valid and appropriate.

        Args:
            field (Field): The field for which the string value is to be
                generated. This should be an instance of the Field class
                that outlines the requirements and constraints for the
                generated string.

        Returns:
            str: A synthetic string value that meets the criteria of the
                specified field. If the generation process encounters an
                issue, an empty string may be returned.
        """
        min_length = field.minLength or 0
        max_length = field.maxLength or 80
        base_pattern = field.pattern

        if base_pattern:
            pattern = f"^[{base_pattern}]{{{min_length},{max_length}}}$"
            return xeger(pattern)
        else:
            string_value = self.generate_faker_value(field.format)

            if not string_value:
                string_value = self.faker.text(max_nb_chars=max_length)

            if len(string_value) < min_length:
                string_value = string_value.ljust(min_length, "x")  # Pad with 'x'
            elif len(string_value) > max_length:
                string_value = string_value[:max_length]  # Truncate to max_length

        return string_value

    def generate_row(
        self,
    ):
        """Generate a single row of dummy data based on the schema."""
        row = []
        for field in self.schema.fields:
            field_name = field.name
            field_data_type = field.dataType

            field = self.model.fields.get(field_name)
            faker_attribute = field.model_dump().get(self.faker_attribute_field)
            faker_value = self.generate_faker_value(faker_attribute)

            # Check the data type of the column and generate appropriate data
            if not field.required and (
                random.random() <= self.null_rate
                or isinstance(field_data_type, types.NullType)
            ):
                row.append(None)
            elif faker_value:
                row.append(faker_value)
            elif isinstance(field_data_type, types.StringType) or field.type == "string":
                row.append(self.generate_string_value(field))
            elif isinstance(field_data_type, types.IntegerType) or field.type == "int" or field.type == "integer":
                row.append(generate_signed_value(field))
            elif isinstance(field_data_type, types.LongType) or field.type == "long":
                row.append(generate_signed_value(field))
            elif isinstance(field_data_type, types.FloatType) or field.type == "float":
                row.append(generate_unsigned_value(field))
            elif isinstance(field_data_type, types.DoubleType) or field.type == "double":
                row.append(generate_unsigned_value(field))
            elif isinstance(field_data_type, types.BooleanType):
                row.append(random.choice([True, False]))
            elif isinstance(field_data_type, types.DateType):  # TODO
                row.append(self.faker.date_this_decade())
            elif isinstance(field_data_type, types.TimestampType):  # TODO
                row.append(self.faker.date_time_this_decade())
            elif isinstance(field_data_type, types.DecimalType):
                row.append(generate_decimal_value(field))
            else:
                row.append(None)  # For unsupported types, append None

        return tuple(row)

    def generate_data(
        self,
        num_rows=10,
    ) -> list[Any]:
        """Generate multiple rows of dummy data based on the schema using parallelization.

        Args:
            num_rows (int): Number of rows to generate.
        """
        data = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.generate_row) for _ in range(num_rows)]

            for future in as_completed(futures):
                data.append(future.result())

        return data

    def generate_dataframe(
        self,
        spark: SparkSession,
        num_rows=10,
    ) -> DataFrame:
        """Generate a PySpark DataFrame from the generated dummy data.

        Args:
            spark (SparkSession): Spark Session.
            num_rows (int): Number of rows to generate.

        Raises:
            ImportError: If pyspark is not installed.
        """
        try:
            data = self.generate_data(num_rows)
            df = spark.createDataFrame(data=data, schema=self.schema)

            return df.orderBy(df.columns[0])
        except ImportError:
            raise ImportError(
                "pyspark is not installed. Please install it using 'pip install pyspark' to use this function."
            )
