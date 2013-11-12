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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.persistance.FinderType;

/**
 *
 * @author salman
 * right now it holds quota info. later we can add more information like access time ( if we want to remove locks from the parent dirs )
 */
public class INodeAttributes {

    public static enum Finder implements FinderType<INodeAttributes> {
        ByPKey;
        @Override
        public Class getType() {
            return INodeAttributes.class;
        }
    }
    
    private long inodeId;
    private long nsQuota; /// NameSpace quota
    private long nsCount;
    private long dsQuota; /// disk space quota
    private long diskspace;

    public INodeAttributes(long inodeId, long nsQuota, long nsCount, long dsQuota, long diskspace) {
        this.inodeId = inodeId;
        this.nsQuota = nsQuota;
        this.nsCount = nsCount;
        this.dsQuota = dsQuota;
        this.diskspace = diskspace;
    }

    public long getInodeId() {
        return inodeId;
    }

    public long getNsQuota() {
        return nsQuota;
    }

    public long getNsCount() {
        return nsCount;
    }

    public long getDsQuota() {
        return dsQuota;
    }

    public long getDiskspace() {
        return diskspace;
    }

    public void setNsQuota(long nsQuota) {
        this.nsQuota = nsQuota;
    }

    public void setNsCount(long nsCount) {
        this.nsCount = nsCount;
    }

    public void setDsQuota(long dsQuota) {
        this.dsQuota = dsQuota;
    }

    public void setDiskspace(long diskspace) {
        this.diskspace = diskspace;
    }
}
