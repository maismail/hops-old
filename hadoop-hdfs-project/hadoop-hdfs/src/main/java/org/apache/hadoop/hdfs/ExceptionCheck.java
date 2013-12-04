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
package org.apache.hadoop.hdfs;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import org.apache.hadoop.ipc.RemoteException;

/**
 *
 * @author salman
 */
public class ExceptionCheck {

    public static boolean isLocalConnectException(Exception e) {
        //for these Exceptions RPC call will be retried
        if (e instanceof ConnectException
                || e instanceof SocketException
                || e instanceof BindException
                || e instanceof UnknownHostException
                || e instanceof SocketTimeoutException
                || e instanceof NoRouteToHostException
                || (e instanceof IOException && e.getMessage().contains("Failed on local exception"))
                || e instanceof NoAliveNamenodeException
                || e instanceof NullPointerException) { // Nullpointer exception as caused by dead locks
            return true;
        }

        if (e instanceof RemoteException) {
            Exception unwrappedException = ((RemoteException) e).unwrapRemoteException(); //unwraps wrapped IOExceptions
            if(unwrappedException instanceof RemoteException){ //unable to unwrap
               unwrappedException =  ((RemoteException) e).unwrapRemoteRuntimeException(); //unwraps wrapped RuntimeExceptions
            }
            
            if(unwrappedException != null && !(unwrappedException instanceof RemoteException)) { 
                if (unwrappedException instanceof PersistanceException
                    || unwrappedException instanceof ClusterJException) {
                        return true;
                }
            }
        }
       return false;
    }
}
