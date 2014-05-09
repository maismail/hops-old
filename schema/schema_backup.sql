-- MySQL dump 10.13  Distrib 5.5.37, for debian-linux-gnu (x86_64)
--
-- Host: cloud11    Database: hop_steffen
-- ------------------------------------------------------
-- Server version	5.6.15-ndb-7.3.4-cluster-gpl

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `block_infos`
--

DROP TABLE IF EXISTS `block_infos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `block_infos`
--

LOCK TABLES `block_infos` WRITE;
/*!40000 ALTER TABLE `block_infos` DISABLE KEYS */;
/*!40000 ALTER TABLE `block_infos` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `corrupt_replicas`
--

DROP TABLE IF EXISTS `corrupt_replicas`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corrupt_replicas` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  PRIMARY KEY (`block_id`,`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `corrupt_replicas`
--

LOCK TABLES `corrupt_replicas` WRITE;
/*!40000 ALTER TABLE `corrupt_replicas` DISABLE KEYS */;
/*!40000 ALTER TABLE `corrupt_replicas` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `encoding_status`
--

DROP TABLE IF EXISTS `encoding_status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `encoding_status` (
  `inode_id` bigint(20) NOT NULL,
  `status` int(11) NOT NULL,
  `codec` varchar(8) DEFAULT NULL,
  `modification_time` bigint(20) NOT NULL,
  `target_replication` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `encoding_status`
--

LOCK TABLES `encoding_status` WRITE;
/*!40000 ALTER TABLE `encoding_status` DISABLE KEYS */;
/*!40000 ALTER TABLE `encoding_status` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `excess_replicas`
--

DROP TABLE IF EXISTS `excess_replicas`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `excess_replicas` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  PRIMARY KEY (`block_id`,`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `excess_replicas`
--

LOCK TABLES `excess_replicas` WRITE;
/*!40000 ALTER TABLE `excess_replicas` DISABLE KEYS */;
/*!40000 ALTER TABLE `excess_replicas` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `inode_attributes`
--

DROP TABLE IF EXISTS `inode_attributes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `inode_attributes` (
  `inodeId` bigint(20) NOT NULL,
  `nsquota` bigint(20) DEFAULT NULL,
  `dsquota` bigint(20) DEFAULT NULL,
  `nscount` bigint(20) DEFAULT NULL,
  `diskspace` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inodeId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inodeId) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `inode_attributes`
--

LOCK TABLES `inode_attributes` WRITE;
/*!40000 ALTER TABLE `inode_attributes` DISABLE KEYS */;
/*!40000 ALTER TABLE `inode_attributes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `inodes`
--

DROP TABLE IF EXISTS `inodes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `inodes` (
  `id` bigint(20) NOT NULL,
  `parent_id` bigint(20) NOT NULL DEFAULT '0',
  `name` varchar(3000) NOT NULL DEFAULT '',
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
  PRIMARY KEY (`parent_id`,`name`),
  KEY `parent_idx` (`parent_id`),
  KEY `inode_idx` (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `inodes`
--

LOCK TABLES `inodes` WRITE;
/*!40000 ALTER TABLE `inodes` DISABLE KEYS */;
/*!40000 ALTER TABLE `inodes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `invalidated_blocks`
--

DROP TABLE IF EXISTS `invalidated_blocks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `invalidated_blocks` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `generation_stamp` bigint(20) DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (block_id) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `invalidated_blocks`
--

LOCK TABLES `invalidated_blocks` WRITE;
/*!40000 ALTER TABLE `invalidated_blocks` DISABLE KEYS */;
/*!40000 ALTER TABLE `invalidated_blocks` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `leader`
--

DROP TABLE IF EXISTS `leader`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `leader` (
  `id` bigint(20) NOT NULL,
  `counter` bigint(20) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `hostname` varchar(25) NOT NULL,
  `avg_request_processing_latency` int(11) DEFAULT NULL,
  `partition_val` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`,`partition_val`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (partition_val) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `leader`
--

LOCK TABLES `leader` WRITE;
/*!40000 ALTER TABLE `leader` DISABLE KEYS */;
/*!40000 ALTER TABLE `leader` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `lease_paths`
--

DROP TABLE IF EXISTS `lease_paths`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `lease_paths` (
  `holder_id` int(11) NOT NULL,
  `path` varchar(256) NOT NULL,
  PRIMARY KEY (`path`),
  KEY `id_idx` (`holder_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `lease_paths`
--

LOCK TABLES `lease_paths` WRITE;
/*!40000 ALTER TABLE `lease_paths` DISABLE KEYS */;
/*!40000 ALTER TABLE `lease_paths` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `leases`
--

DROP TABLE IF EXISTS `leases`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `leases` (
  `holder` varchar(255) NOT NULL,
  `last_update` bigint(20) DEFAULT NULL,
  `holder_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`holder`),
  KEY `holderid_idx` (`holder_id`),
  KEY `update_idx` (`last_update`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `leases`
--

LOCK TABLES `leases` WRITE;
/*!40000 ALTER TABLE `leases` DISABLE KEYS */;
/*!40000 ALTER TABLE `leases` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pending_blocks`
--

DROP TABLE IF EXISTS `pending_blocks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pending_blocks` (
  `block_id` bigint(20) NOT NULL,
  `time_stamp` bigint(20) NOT NULL,
  `num_replicas_in_progress` int(11) NOT NULL,
  PRIMARY KEY (`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pending_blocks`
--

LOCK TABLES `pending_blocks` WRITE;
/*!40000 ALTER TABLE `pending_blocks` DISABLE KEYS */;
/*!40000 ALTER TABLE `pending_blocks` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `replica_under_constructions`
--

DROP TABLE IF EXISTS `replica_under_constructions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `replica_under_constructions` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `state` int(11) DEFAULT NULL,
  `replica_index` int(11) NOT NULL,
  PRIMARY KEY (`block_id`,`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `replica_under_constructions`
--

LOCK TABLES `replica_under_constructions` WRITE;
/*!40000 ALTER TABLE `replica_under_constructions` DISABLE KEYS */;
/*!40000 ALTER TABLE `replica_under_constructions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `replicas`
--

DROP TABLE IF EXISTS `replicas`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `replicas` (
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `replica_index` int(11) NOT NULL,
  PRIMARY KEY (`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (storage_id) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `replicas`
--

LOCK TABLES `replicas` WRITE;
/*!40000 ALTER TABLE `replicas` DISABLE KEYS */;
/*!40000 ALTER TABLE `replicas` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `storage_id_map`
--

DROP TABLE IF EXISTS `storage_id_map`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `storage_id_map` (
  `storage_id` varchar(128) NOT NULL,
  `sid` int(11) NOT NULL,
  PRIMARY KEY (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `storage_id_map`
--

LOCK TABLES `storage_id_map` WRITE;
/*!40000 ALTER TABLE `storage_id_map` DISABLE KEYS */;
/*!40000 ALTER TABLE `storage_id_map` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `under_replicated_blocks`
--

DROP TABLE IF EXISTS `under_replicated_blocks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `under_replicated_blocks` (
  `block_id` bigint(20) NOT NULL,
  `level` int(11) DEFAULT NULL,
  PRIMARY KEY (`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `under_replicated_blocks`
--

LOCK TABLES `under_replicated_blocks` WRITE;
/*!40000 ALTER TABLE `under_replicated_blocks` DISABLE KEYS */;
/*!40000 ALTER TABLE `under_replicated_blocks` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `variables`
--

DROP TABLE IF EXISTS `variables`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `variables` (
  `id` int(11) NOT NULL,
  `value` varbinary(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `variables`
--

LOCK TABLES `variables` WRITE;
/*!40000 ALTER TABLE `variables` DISABLE KEYS */;
/*!40000 ALTER TABLE `variables` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2014-05-09 22:41:37
