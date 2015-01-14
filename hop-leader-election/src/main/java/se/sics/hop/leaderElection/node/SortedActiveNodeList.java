/*
 * Copyright 2013 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.sics.hop.leaderElection.node;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
//import org.apache.hadoop.net.NetUtils;

/**
 *
 * @author salman
 */
public interface SortedActiveNodeList {

  public boolean isEmpty();

  public int size();

  public List<ActiveNode> getSortedActiveNodes();

  public List<ActiveNode> getActiveNodes();

  public ActiveNode getActiveNode(InetSocketAddress address);

  public ActiveNode getLeader();
}
