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
package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.INodeAttributesDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author salman
 */
public class INodeAttributesClusterj extends INodeAttributesDataAccess {

    @PersistenceCapable(table = TABLE_NAME)
    public interface INodeAttributesDTO {

        @PrimaryKey
        @Column(name = ID)
        long getId();

        void setId(long id);

        @Column(name = NSQUOTA)
        long getNSQuota();

        void setNSQuota(long nsquota);

        @Column(name = DSQUOTA)
        long getDSQuota();

        void setDSQuota(long dsquota);

        @Column(name = NSCOUNT)
        long getNSCount();

        void setNSCount(long nscount);

        @Column(name = DISKSPACE)
        long getDiskspace();

        void setDiskspace(long diskspace);
    }
    private ClusterjConnector connector = ClusterjConnector.INSTANCE;

    @Override
    public INodeAttributes findAttributesByPk(long inodeId) throws StorageException {
        Session session = connector.obtainSession();
        try {
            INodeAttributesDTO dto = session.find(INodeAttributesDTO.class, inodeId);
            INodeAttributes iNodeAttributes = makeINodeAttributes(dto);
            return iNodeAttributes;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<INodeAttributes> modified, Collection<INodeAttributes> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            for (INodeAttributes attr : removed) {
                INodeAttributesDTO persistable = session.newInstance(INodeAttributesDTO.class, attr.getInodeId());
                session.deletePersistent(persistable);
            }

            for (INodeAttributes attr : modified) {
                INodeAttributesDTO persistable = persistable = createPersistable(attr, session);
                session.savePersistent(persistable);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private INodeAttributesDTO createPersistable(INodeAttributes attribute, Session session) {
        INodeAttributesDTO dto = session.newInstance(INodeAttributesDTO.class);
        dto.setId(attribute.getInodeId());
        dto.setNSQuota(attribute.getNsQuota());
        dto.setNSCount(attribute.getNsCount());
        dto.setDSQuota(attribute.getDsQuota());
        dto.setDiskspace(attribute.getDiskspace());
        return dto;
    }

    private INodeAttributes makeINodeAttributes(INodeAttributesDTO dto) {
        if (dto == null) {
            return null;
        }
        INodeAttributes iNodeAttributes = new INodeAttributes(
                dto.getId(),
                dto.getNSQuota(),
                dto.getNSCount(),
                dto.getDSQuota(),
                dto.getDiskspace());
        return iNodeAttributes;

    }
}
