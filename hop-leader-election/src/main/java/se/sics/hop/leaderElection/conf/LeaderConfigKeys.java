/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package se.sics.hop.leaderElection.conf;

//import org.apache.hadoop.classification.InterfaceAudience;
//import org.apache.hadoop.fs.CommonConfigurationKeys;
/**
 * This class contains constants for configuration keys used in leader election.
 *
 */
//@InterfaceAudience.Private
public class LeaderConfigKeys /*extends CommonConfigurationKeys*/ {

  public static final String DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY
          = "dfs.leader.check.interval";
  public static final int DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT
          = 3 * 1000; // 3 second 

  public static final String DFS_LEADER_MISSED_HB_THRESHOLD_KEY
          = "dfs.leader.missed.hb";
  public static final int DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT = 1;

  public static final String DFS_SET_PARTITION_KEY_ENABLED
          = "dfs.ndb.setpartitionkey.enabled";
  public static final boolean DFS_SET_PARTITION_KEY_ENABLED_DEFAULT = false;
}
