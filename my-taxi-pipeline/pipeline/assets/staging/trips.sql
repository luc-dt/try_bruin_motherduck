/* @bruin

name: staging.trips
type: duckdb.sql
connection: motherduck-prod

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: create+replace
  -- incremental_key: pickup_datetime
  -- time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "Timestamp when the trip started"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Timestamp when the trip ended"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone ID for the pickup location"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone ID for the dropoff location"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Base fare amount in USD"
    primary_key: true
    checks:
      - name: non_negative
  - name: taxi_type
    type: string
    description: "Type of taxi: yellow or green"
    checks:
      - name: not_null
  - name: payment_type_id
    type: integer
    description: "Numeric payment type identifier"
  - name: payment_type_name
    type: string
    description: "Human-readable payment type name from lookup table"
  - name: passenger_count
    type: integer
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Total amount charged to the passenger"
    checks:
      - name: non_negative
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the record was extracted from the source"

custom_checks:
  - name: no_duplicate_trips
    description: "Ensure no duplicate trips exist in the staging table within the time window"
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT (pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount, taxi_type))
      FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 0

@bruin */

WITH raw AS (
    SELECT
        COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)   AS pickup_datetime,
        COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS dropoff_datetime,
        COALESCE(pu_location_id, NULL)                         AS pickup_location_id,
        COALESCE(do_location_id, NULL)                         AS dropoff_location_id,
        passenger_count,
        trip_distance,
        COALESCE(payment_type, NULL)                           AS payment_type_id,
        fare_amount,
        total_amount,
        taxi_type,
        extracted_at
    FROM ingestion.trips
    WHERE COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) >= '{{ start_datetime }}'
      AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) < '{{ end_datetime }}'
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount, taxi_type
            ORDER BY extracted_at DESC
        ) AS rn
    FROM raw
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND fare_amount >= 0
      AND total_amount >= 0
)

SELECT
    d.pickup_datetime,
    d.dropoff_datetime,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.passenger_count,
    d.trip_distance,
    d.payment_type_id,
    pl.payment_type_name,
    d.fare_amount,
    d.total_amount,
    d.taxi_type,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup pl
    ON d.payment_type_id = pl.payment_type_id
WHERE d.rn = 1
