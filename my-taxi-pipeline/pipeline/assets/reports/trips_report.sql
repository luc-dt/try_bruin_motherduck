/* @bruin

name: reports.trips_report
type: duckdb.sql
connection: motherduck-prod

depends:
  - staging.trips

materialization:
  type: table
  strategy: delete+insert
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "Date of the trip pickup (truncated to day)"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi: yellow or green"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment type name"
    primary_key: true
  - name: trip_count
    type: bigint
    description: "Number of trips for this date/taxi_type/payment_type combination"
    checks:
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: "Total number of passengers"
    checks:
      - name: non_negative
  - name: total_distance_miles
    type: float
    description: "Total trip distance in miles"
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: "Total base fare amount in USD"
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Total amount charged to passengers in USD"
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: float
    description: "Average base fare amount per trip in USD"
    checks:
      - name: non_negative
  - name: avg_trip_distance_miles
    type: float
    description: "Average trip distance in miles"
    checks:
      - name: non_negative

@bruin */

SELECT
    CAST(pickup_datetime AS DATE)           AS pickup_date,
    taxi_type,
    COALESCE(payment_type_name, 'unknown')  AS payment_type_name,
    COUNT(*)                                AS trip_count,
    SUM(passenger_count)                    AS total_passengers,
    SUM(trip_distance)                      AS total_distance_miles,
    SUM(fare_amount)                        AS total_fare_amount,
    SUM(total_amount)                       AS total_amount,
    AVG(fare_amount)                        AS avg_fare_amount,
    AVG(trip_distance)                      AS avg_trip_distance_miles
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    COALESCE(payment_type_name, 'unknown')
