"""DataSpawner example."""

from datacontract.data_contract import DataContract
from faker_airtravel import AirTravelProvider
from pyspark.sql import SparkSession

from dataspawner.data_spawner import DataSpawner

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    data_contract = DataContract(
        data_contract_file="datacontract.yaml",
        spark=spark,
    )

    spawner = DataSpawner(
        data_contract=data_contract,
        model_name="example_model",
        custom_faker_providers=[AirTravelProvider],
        locales=["en_US"],
        seed=12345,
        max_workers=4,
    )

    df = spawner.generate_dataframe(spark=spark)
    df.show(
        truncate=False,
        vertical=True,
    )
