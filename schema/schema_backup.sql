delimiter $$

CREATE TABLE `block_infos` (
  `block_id` bigint(20) NOT NULL,
  `block_index` int(11) DEFAULT NULL,
  `inode_id` bigint(20) NOT NULL DEFAULT '0',
  `num_bytes` bigint(20) DEFAULT NULL,
  `generation_stamp` bigint(20) DEFAULT NULL,
  `block_under_construction_state` int(11) DEFAULT NULL,
  `time_stamp` bigint(20) DEFAULT NULL,
  `primary_node_index` int(11) DEFAULT NULL,
  `block_recovery_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`block_id`),
  KEY `inode_idx` (`inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `corrupt_replicas` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` varchar(128) NOT NULL,
  PRIMARY KEY (`block_id`,`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `excess_replicas` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` varchar(128) NOT NULL,
  PRIMARY KEY (`block_id`,`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `inode_attributes` (
  `inodeId` bigint(20) NOT NULL,
  `nsquota` bigint(20) DEFAULT NULL,
  `dsquota` bigint(20) DEFAULT NULL,
  `nscount` bigint(20) DEFAULT NULL,
  `diskspace` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inodeId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inodeId) */$$


delimiter $$

CREATE TABLE `inodes` (
  `id` bigint(20) NOT NULL,
  `name` varchar(3000) DEFAULT NULL,
  `parent_id` bigint(20) DEFAULT NULL,
  `is_dir` int(11) NOT NULL,
  `modification_time` bigint(20) DEFAULT NULL,
  `access_time` bigint(20) DEFAULT NULL,
  `permission` varbinary(128) DEFAULT NULL,
  `is_under_construction` int(11) NOT NULL,
  `client_name` varchar(45) DEFAULT NULL,
  `client_machine` varchar(45) DEFAULT NULL,
  `client_node` varchar(45) DEFAULT NULL,
  `is_closed_file` int(11) DEFAULT NULL,
  `header` bigint(20) DEFAULT NULL,
  `is_dir_with_quota` int(11) NOT NULL,
  `symlink` varchar(3000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `path_lookup_idx` (`name`,`parent_id`),
  KEY `parent_idx` (`parent_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `invalidated_blocks` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` varchar(128) NOT NULL,
  `generation_stamp` bigint(20) DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (block_id) */$$


delimiter $$

CREATE TABLE `leader` (
  `id` bigint(20) NOT NULL,
  `counter` bigint(20) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `hostname` varchar(25) NOT NULL,
  `avg_request_processing_latency` int(11) DEFAULT NULL,
  `partition_val` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`,`partition_val`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (partition_val) */$$


delimiter $$

CREATE TABLE `lease_paths` (
  `holder_id` int(11) NOT NULL,
  `path` varchar(256) NOT NULL,
  PRIMARY KEY (`path`),
  KEY `id_idx` (`holder_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `leases` (
  `holder` varchar(255) NOT NULL,
  `last_update` bigint(20) DEFAULT NULL,
  `holder_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`holder`),
  KEY `holderid_idx` (`holder_id`),
  KEY `update_idx` (`last_update`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `pending_blocks` (
  `block_id` bigint(20) NOT NULL,
  `time_stamp` bigint(20) NOT NULL,
  `num_replicas_in_progress` int(11) NOT NULL,
  PRIMARY KEY (`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `replica_under_constructions` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` varchar(128) NOT NULL,
  `state` int(11) DEFAULT NULL,
  `replica_index` int(11) NOT NULL,
  PRIMARY KEY (`block_id`,`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `replicas` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` varchar(128) NOT NULL,
  `replica_index` int(11) NOT NULL,
  PRIMARY KEY (`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (storage_id) */$$


delimiter $$

CREATE TABLE `under_replicated_blocks` (
  `block_id` bigint(20) NOT NULL,
  `level` int(11) DEFAULT NULL,
  PRIMARY KEY (`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `variables` (
  `id` int(11) NOT NULL,
  `value` varbinary(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `storage_id_map` (
  `storage_id` varchar(128) NOT NULL,
  `sid` int(11) NOT NULL,
  PRIMARY KEY (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

