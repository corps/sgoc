CREATE TABLE `log_entries` (
  root_key     VARCHAR(32) NOT NULL,
  sequence     BIGINT      NOT NULL,
  timestamp    BIGINT      NOT NULL,
  PRIMARY KEY (root_key, sequence),
  UNIQUE KEY `idx_unique_user_token_timestamp` (root_key, timestamp)
);

CREATE TABLE `objects` (
  root_key   VARCHAR(32)    NOT NULL,
  uuid       VARCHAR(32)    NOT NULL,
  object     LONGBLOB       NOT NULL,
  deleted    BOOLEAN        NOT NULL COMMENT 'Only used for filtering on initial import.',
  timestamp  BIGINT         NOT NULL,
  PRIMARY KEY (root_key, uuid),
  KEY `idx_objects_by_timestamp` (root_key, timestamp)
);

CREATE TABLE `index_entries` (
  root_key       VARCHAR(32)    NOT NULL,
  uuid           VARCHAR(32)    NOT NULL,
  index_key      VARBINARY(767) NOT NULL COMMENT 'binary of the proto',
  index_value    VARBINARY(767) NOT NULL COMMENT 'binary of the proto',
  FOREIGN KEY `fk_referenced_object` (root_key, uuid) REFERENCES `objects` (root_key, uuid),
  PRIMARY KEY (root_key, index_key, index_value, uuid)
);
