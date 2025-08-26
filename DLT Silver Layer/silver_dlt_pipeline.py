import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format(
        "delta"
    ).load(
        "/Volumes/workspace/bronze/bronzevolume/bookings/data/"
    )
    return df

@dlt.table(
    name="trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_bookings")
    df= df.withColumn("amount",col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate",current_timestamp())\
        .withColumn("booking_date",to_date(col("booking_date"))) \
        .drop("_rescued_data")

    return df

rules = {
    "rule1" : "booking_id is not null",
    "rule2" : "passenger_id is not null"
}

@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df=spark.readStream.table("trans_bookings")
    return df   



###########################################

@dlt.view(
name = "trans_flights"
)

def trans_flights():
    df = spark.readStream.format(
        "delta"
    ).load(
        "/Volumes/workspace/bronze/bronzevolume/flights/data/"
    )
    df= df.drop("_rescued_data")\
    .withColumn("modifiedDate",current_timestamp())

    return df


dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
target = "silver_flights",
source = "trans_flights",
keys = ["flight_id"],
sequence_by=col("modifiedDate"),
stored_as_scd_type = 1

)

########################


@dlt.view(
name = "trans_passengers"
)
def trans_passengers():
    df = spark.readStream.format(
        "delta"
    ).load(
        "/Volumes/workspace/bronze/bronzevolume/customers/data/"
    )
    df= df.drop("_rescued_data")\
    .withColumn("modifiedDate",current_timestamp())

    return df


dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
target = "silver_passengers",
source = "trans_passengers",
keys = ["passenger_id"],
sequence_by=col("modifiedDate"),
stored_as_scd_type = 1
)

########################

@dlt.view(
name = "trans_airports"
)
def trans_airports():
    df = spark.readStream.format(
        "delta"
    ).load(
        "/Volumes/workspace/bronze/bronzevolume/airports/data/"
    )
    df= df.drop("_rescued_data")\
        .withColumn("modifiedDate",current_timestamp())
    return df



dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
target = "silver_airports",
source = "trans_airports",
keys = ["airport_id"],
sequence_by=col("modifiedDate"),
stored_as_scd_type = 1
)


#########################
#silver business view
@dlt.table(
    name="silver_business"
)

def silver_business():
    df = spark.readStream.table("silver_bookings")
    df = df.join(dlt.readStream("silver_flights"),["flight_id"])\
        .join(dlt.readStream("silver_passengers"),["passenger_id"])\
        .join(dlt.readStream("silver_airports"),["airport_id"])\
        .drop("modifiedDate")\
        .drop("_rescued_data")
    return df







