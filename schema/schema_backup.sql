delimiter $$

CREATE TABLE `block_infos` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `block_index` int(11) DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  `generation_stamp` bigint(20) DEFAULT NULL,
  `block_under_construction_state` int(11) DEFAULT NULL,
  `time_stamp` bigint(20) DEFAULT NULL,
  `primary_node_index` int(11) DEFAULT NULL,
  `block_recovery_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `block_lookup_table` (
  `block_id` bigint(20) NOT NULL,
  `inode_id` int(11) NOT NULL,
  PRIMARY KEY (`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `corrupt_replicas` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `timestamp` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `timestamp` (`timestamp`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `excess_replicas` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `inode_attributes` (
  `inodeId` int(11) NOT NULL,
  `nsquota` bigint(20) DEFAULT NULL,
  `dsquota` bigint(20) DEFAULT NULL,
  `nscount` bigint(20) DEFAULT NULL,
  `diskspace` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inodeId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `inodes` (
  `id` int(11) NOT NULL,
  `parent_id` int(11) NOT NULL DEFAULT '0',
  `name` varchar(3000) NOT NULL DEFAULT '',
  `modification_time` bigint(20) DEFAULT NULL,
  `access_time` bigint(20) DEFAULT NULL,
  `permission` varbinary(128) DEFAULT NULL,
  `client_name` varchar(100) DEFAULT NULL,
  `client_machine` varchar(100) DEFAULT NULL,
  `client_node` varchar(100) DEFAULT NULL,
  `generation_stamp` int(11) DEFAULT NULL,
  `header` bigint(20) DEFAULT NULL,
  `symlink` varchar(3000) DEFAULT NULL,
  `dir` bit(1) NOT NULL,
  `quota_enabled` bit(1) NOT NULL,
  `under_construction` bit(1) NOT NULL,
  `subtree_locked` bit(1) DEFAULT NULL,
  `subtree_lock_owner` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`parent_id`,`name`),
  KEY `inode_idx` (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (parent_id) */$$


delimiter $$

CREATE TABLE `invalidated_blocks` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `generation_stamp` bigint(20) DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `leader` (
  `id` bigint(20) NOT NULL,
  `counter` bigint(20) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `hostname` varchar(25) NOT NULL,
  `httpAddress` varchar(100) DEFAULT NULL,
  `partition_val` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`,`partition_val`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (partition_val) */$$


delimiter $$

CREATE TABLE `lease_paths` (
  `holder_id` int(11) NOT NULL,
  `path` varchar(256) NOT NULL,
  `part_key` int(11) NOT NULL,
  PRIMARY KEY (`path`,`part_key`),
  KEY `holder_idx` (`holder_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (part_key) */$$


delimiter $$

CREATE TABLE `leases` (
  `holder` varchar(255) NOT NULL,
  `part_key` int(11) NOT NULL,
  `last_update` bigint(20) DEFAULT NULL,
  `holder_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`holder`,`part_key`),
  KEY `holderid_idx` (`holder_id`),
  KEY `update_idx` (`last_update`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (part_key) */$$


delimiter $$

CREATE TABLE `misreplicated_range_queue` (
  `range` varchar(120) NOT NULL,
  PRIMARY KEY (`range`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `path_memcached` (
  `path` varchar(128) NOT NULL,
  `inodeids` varbinary(13500) NOT NULL,
  PRIMARY KEY (`path`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `pending_blocks` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `time_stamp` bigint(20) NOT NULL,
  `num_replicas_in_progress` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `replica_under_constructions` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `state` int(11) DEFAULT NULL,
  `replica_index` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `replicas` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `replica_index` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `safe_blocks` (
  `id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `storage_id_map` (
  `storage_id` varchar(128) NOT NULL,
  `sid` int(11) NOT NULL,
  PRIMARY KEY (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `under_replicated_blocks` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `level` int(11) DEFAULT NULL,
  `timestamp` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`),
  KEY `level` (`level`,`timestamp`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `variables` (
  `id` int(11) NOT NULL,
  `value` varbinary(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `quota_update` (
  `id` int(11) NOT NULL,
  `inode_id` int(11) NOT NULL,
  `namespace_delta` bigint(20) DEFAULT NULL,
  `diskspace_delta` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `encoding_status` (
  `inode_id` int(11) NOT NULL,
  `status` int(11) DEFAULT NULL,
  `codec` varchar(8) DEFAULT NULL,
  `target_replication` smallint(11) DEFAULT NULL,
  `parity_status` int(11) DEFAULT NULL,
  `status_modification_time` bigint(20) DEFAULT NULL,
  `parity_status_modification_time` bigint(20) DEFAULT NULL,
  `parity_inode_id` int(11) DEFAULT NULL,
  `parity_file_name` char(36) DEFAULT NULL,
  `lost_blocks` int(11) DEFAULT 0,
  `lost_parity_blocks` int(11) DEFAULT 0,
  `revoked` bit(1) DEFAULT 0,
  PRIMARY KEY (`inode_id`),
  UNIQUE KEY `parity_inode_id` (`parity_inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `block_checksum` (
  `inode_id` int(11) NOT NULL,
  `block_index` int(11) NOT NULL,
  `checksum` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_index`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$
