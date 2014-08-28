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
package org.apache.hadoop.hdfs.server.protocol;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.net.NetUtils;

/**
 *
 * @author salman
 */
public class SortedActiveNamenodeList {
    private List<ActiveNamenode> listActiveNamenodes;

    public SortedActiveNamenodeList(List<ActiveNamenode> listActiveNamenodes) {
        if (listActiveNamenodes == null) {
            throw new NullPointerException("List of active namenodes was null");
        }
        this.listActiveNamenodes = listActiveNamenodes;
        Collections.sort(listActiveNamenodes);
    }
   
    public int size(){
        return listActiveNamenodes.size();
    }
    
    public List<ActiveNamenode> getActiveNamenodes(){
        return listActiveNamenodes;
    }
    
    public ActiveNamenode getActiveNamenode(InetSocketAddress address){
      for(ActiveNamenode namenode : listActiveNamenodes){
        if(namenode.getInetSocketAddress().equals(address)){
          return namenode;
        }
      }
      return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Active Namenodes are ");
        if(listActiveNamenodes == null || listActiveNamenodes.size() == 0){
            sb.append(" EMPTY ");
        }
        else{
            for(int i = 0 ; i < listActiveNamenodes.size(); i++)
            {
                ActiveNamenode ann = listActiveNamenodes.get(i);
                sb.append("[ id: "+ann.getId());
                //sb.append(" addr: "+ann.getIpAddress()+":"+ann.getPort());
                sb.append(" addr: "+NetUtils.getHostPortString(ann.getInetSocketAddress()));
                sb.append(" ] ");
            }
        }
        return sb.toString();
    }
    
    public ActiveNamenode getLeader(){ //in our case the node wiht smallest id is the leader
        if(listActiveNamenodes == null || listActiveNamenodes.size() == 0){
            return null;
        }
        return listActiveNamenodes.get(0);
    }
    
    
}
