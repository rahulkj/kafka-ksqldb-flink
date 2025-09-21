COMBINE TOPICS TO ENRICH DATA
---

## USING KSQLDB

```
CREATE TABLE USER (
  userName VARCHAR PRIMARY KEY,
  age INTEGER,
  orderNumber VARCHAR
) WITH (
  KAFKA_TOPIC='users',
  PARTITIONS=6,
  VALUE_FORMAT='AVRO'
);
```

```
CREATE TABLE ORDER (
  orderNumber VARCHAR PRIMARY KEY,
  productName VARCHAR,
  orderSize INT
) WITH (
  KAFKA_TOPIC='orders',
  PARTITIONS=6,
  VALUE_FORMAT='AVRO'
);
```

```
CREATE TABLE USER_ORDER WITH (KAFKA_TOPIC='user_order', VALUE_FORMAT='AVRO', VALUE_SCHEMA_FULL_NAME='io.demo.model.UserOrder') AS 
  SELECT
    u.userName as ROWKEY,
    AS_VALUE(u.userName) as userName, 
    o.orderSize as orderSize, 
    o.orderNumber as orderNumber, 
    o.productName as productName 
  FROM USER u 
  LEFT JOIN ORDER o on u.orderNumber = o.orderNumber
  EMIT CHANGES;
```

### Cleanup

```
DROP TABLE USER_ORDER;

confluent login --save

confluent environment use env-m56607

confluent kafka cluster list

confluent kafka cluster use lkc-mmwq21

confluent kafka topic list

confluent kafka topic delete userorder

confluent schema-registry schema list

confluent schema-registry schema delete --subject userorder-value --version 10 --permanent --force
```

## USING FLINK

```
INSERT INTO `user_order`
SELECT
  CAST(u.`userName` AS BYTES),
  u.`userName`,
  o.`orderSize`,
  u.`orderNumber`,
  o.`productName`
FROM
  `users` u
LEFT JOIN `orders` o
ON
  u.orderNumber = o.orderNumber;
```

```
SELECT
  userName,
  ARRAY_AGG(orderNumber) AS allOrderNumbers,
  ARRAY_AGG(orderSize) AS allOrders
FROM
  userorder
GROUP BY
  userName;
```

```
CREATE TABLE `rj`.`cluster`.`users` (
  `key` VARBINARY(2147483647),
  `userName` VARCHAR(2147483647) NOT NULL COMMENT 'User Name',
  `age` INTEGER NOT NULL COMMENT 'age',
  `orderNumber` VARCHAR(2147483647) NOT NULL COMMENT 'Order Number'
)
DISTRIBUTED BY HASH(`key`) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.compaction.time' = '0 ms',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.retention.size' = '1 mb',
  'kafka.retention.time' = '1 h',
  'key.format' = 'raw',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
)

CREATE TABLE `rj`.`cluster`.`orders` (
  `key` VARBINARY(2147483647),
  `orderNumber` VARCHAR(2147483647) NOT NULL COMMENT 'Order Number',
  `productName` VARCHAR(2147483647) NOT NULL COMMENT 'Product name',
  `orderSize` INTEGER NOT NULL COMMENT 'Order Size'
)
DISTRIBUTED BY HASH(`key`) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.compaction.time' = '0 ms',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.retention.size' = '1 mb',
  'kafka.retention.time' = '1 h',
  'key.format' = 'raw',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
)

CREATE TABLE `rj`.`cluster`.`user_order` (
  `key` VARBINARY(2147483647),
  `userName` VARCHAR(2147483647) NOT NULL COMMENT 'User Name',
  `orderSize` INT NOT NULL COMMENT 'Order Size',
  `orderNumber` VARCHAR(2147483647) NOT NULL COMMENT 'Order Number',
  `productName` VARCHAR(2147483647) NOT NULL COMMENT 'Product name'
)
DISTRIBUTED BY HASH(`key`) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'retract',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'compact',
  'kafka.compaction.time' = '0 ms',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.retention.size' = '1 mb',
  'kafka.retention.time' = '1 h',
  'key.format' = 'raw',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
)
```

Navigate to Schema Registry and update the schema with the values from
* [User.avsc](./src/main/resources/avro/User.avsc)
* [Order.avsc](./src/main/resources/avro/Order.avsc)
* [UserOrder.avsc](./src/main/resources/avro/UserOrder.avsc)

Set the environment variables:
* KAFKA_SERVERS
* KAFKA_CLIENT_ID
* KAFKA_CLIENT_SECRET
* SR_URL
* SR_CLIENT_ID
* SR_CLIENT_SECRET