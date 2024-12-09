WITH
    [StreamData]
AS (
   SELECT    *    FROM
      [DeviceDataStream]
    WHERE       [ObjectType] IS NULL -- Filter out device info and command responses)
SELECT   EventProcessedUtcTime,
    PartitionId,
    EventEnqueuedUtcTime,
    IoTHub,
    employee_id,
    first_name,
    last_name,
    age,
    salary
INTO    [Telemetry]
FROM    [StreamData]
WHERE type = 'entity1'   --Table 1
    PartitionId,
    EventEnqueuedUtcTime,
    PersonID,
    FirstName,
    LastName,
    City,
    height
INTO    [TelemetryP]
FROM    [StreamData]
WHERE type = 'entity2'  --Table 2
