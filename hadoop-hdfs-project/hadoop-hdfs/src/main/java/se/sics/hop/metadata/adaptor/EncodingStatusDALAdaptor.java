package se.sics.hop.metadata.adaptor;

import se.sics.hop.erasure_coding.EncodingPolicy;
import se.sics.hop.erasure_coding.EncodingStatus;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.DALAdaptor;
import se.sics.hop.metadata.hdfs.dal.EncodingStatusDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopEncodingStatus;

import java.util.Collection;

public class EncodingStatusDALAdaptor extends DALAdaptor<EncodingStatus, HopEncodingStatus>
      implements EncodingStatusDataAccess<EncodingStatus> {

  private final EncodingStatusDataAccess<HopEncodingStatus> dataAccess;

  public EncodingStatusDALAdaptor(EncodingStatusDataAccess<HopEncodingStatus> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public HopEncodingStatus convertHDFStoDAL(EncodingStatus encodingStatus)
      throws StorageException {
    if (encodingStatus == null) {
      return null;
    }

    HopEncodingStatus converted = new HopEncodingStatus();
    converted.setInodeId(encodingStatus.getInodeId());
    converted.setParityInodeId(encodingStatus.getParityInodeId());
    converted.setCodec(encodingStatus.getEncodingPolicy().getCodec());
    converted.setTargetReplication(encodingStatus.getEncodingPolicy().getTargetReplication());
    converted.setStatus(convertStatus(encodingStatus.getStatus()));
    converted.setStatusModificationTime(encodingStatus.getStatusModificationTime());
    converted.setParityStatus(convertParityStatus(encodingStatus.getParityStatus()));
    converted.setParityStatusModificationTime(encodingStatus.getParityStatusModificationTime());
    converted.setParityFileName(encodingStatus.getParityFileName());
    converted.setLostBlocks(encodingStatus.getLostBlocks());
    converted.setLostParityBlocks(encodingStatus.getLostParityBlocks());
    converted.setRevoked(encodingStatus.getRevoked());
    return converted;
  }

  @Override
  public EncodingStatus convertDALtoHDFS(HopEncodingStatus hopEncodingStatus)
      throws StorageException {
    if (hopEncodingStatus == null) {
      return null;
    }

    EncodingStatus converted = new EncodingStatus();
    converted.setInodeId(hopEncodingStatus.getInodeId());
    converted.setParityInodeId(hopEncodingStatus.getParityInodeId());
    EncodingPolicy policy = new EncodingPolicy(hopEncodingStatus.getCodec(), hopEncodingStatus.getTargetReplication());
    converted.setEncodingPolicy(policy);
    converted.setStatus(convertStatus(hopEncodingStatus.getStatus()));
    converted.setStatusModificationTime(hopEncodingStatus.getStatusModificationTime());
    converted.setParityStatus(convertParityStatus(hopEncodingStatus.getParityStatus()));
    converted.setParityStatusModificationTime(hopEncodingStatus.getParityStatusModificationTime());
    converted.setParityFileName(hopEncodingStatus.getParityFileName());
    converted.setLostBlocks(hopEncodingStatus.getLostBlocks());
    converted.setLostParityBlocks(hopEncodingStatus.getLostParityBlocks());
    converted.setRevoked(hopEncodingStatus.getRevoked());
    return converted;
  }

  private EncodingStatus.Status convertStatus(Integer status) {
    if (status == null) {
      return null;
    }

    switch (status) {
      case HopEncodingStatus.ENCODING_REQUESTED:
        return EncodingStatus.Status.ENCODING_REQUESTED;
      case HopEncodingStatus.ENCODING_ACTIVE:
        return EncodingStatus.Status.ENCODING_ACTIVE;
      case HopEncodingStatus.ENCODING_CANCELED:
        return EncodingStatus.Status.ENCODING_CANCELED;
      case HopEncodingStatus.ENCODING_FAILED:
        return EncodingStatus.Status.ENCODING_FAILED;
      case HopEncodingStatus.ENCODED:
        return EncodingStatus.Status.ENCODED;
      case HopEncodingStatus.REPAIR_REQUESTED:
        return EncodingStatus.Status.REPAIR_REQUESTED;
      case HopEncodingStatus.REPAIR_ACTIVE:
        return EncodingStatus.Status.REPAIR_ACTIVE;
      case HopEncodingStatus.REPAIR_CANCELED:
        return EncodingStatus.Status.REPAIR_CANCELED;
      case HopEncodingStatus.REPAIR_FAILED:
        return EncodingStatus.Status.REPAIR_FAILED;
      case HopEncodingStatus.DELETED:
        return EncodingStatus.Status.DELETED;
      default:
        throw new UnsupportedOperationException("Trying to convert an unknown status");
    }
  }

  private Integer convertStatus(EncodingStatus.Status status) {
    if (status == null) {
      return null;
    }

    switch (status) {
      case ENCODING_REQUESTED:
        return HopEncodingStatus.ENCODING_REQUESTED;
      case ENCODING_ACTIVE:
        return HopEncodingStatus.ENCODING_ACTIVE;
      case ENCODING_CANCELED:
        return HopEncodingStatus.ENCODING_CANCELED;
      case ENCODING_FAILED:
        return HopEncodingStatus.ENCODING_FAILED;
      case ENCODED:
        return HopEncodingStatus.ENCODED;
      case REPAIR_REQUESTED:
        return HopEncodingStatus.REPAIR_REQUESTED;
      case REPAIR_ACTIVE:
        return HopEncodingStatus.REPAIR_ACTIVE;
      case REPAIR_CANCELED:
        return HopEncodingStatus.REPAIR_CANCELED;
      case REPAIR_FAILED:
        return  HopEncodingStatus.REPAIR_FAILED;
      case DELETED:
        return HopEncodingStatus.DELETED;
      default:
        throw new UnsupportedOperationException("Trying to convert an unknown status");
    }
  }

  private EncodingStatus.ParityStatus convertParityStatus(Integer status) {
    if (status == null) {
      return null;
    }

    switch (status) {
      case HopEncodingStatus.PARITY_HEALTHY:
        return EncodingStatus.ParityStatus.HEALTHY;
      case HopEncodingStatus.PARITY_REPAIR_REQUESTED:
        return EncodingStatus.ParityStatus.REPAIR_REQUESTED;
      case HopEncodingStatus.PARITY_REPAIR_ACTIVE:
        return EncodingStatus.ParityStatus.REPAIR_ACTIVE;
      case HopEncodingStatus.PARITY_REPAIR_CANCELED:
        return EncodingStatus.ParityStatus.REPAIR_CANCELED;
      case HopEncodingStatus.PARITY_REPAIR_FAILED:
        return EncodingStatus.ParityStatus.REPAIR_FAILED;
      default:
        throw new UnsupportedOperationException("Trying to convert an unknown status");
    }
  }

  private Integer convertParityStatus(EncodingStatus.ParityStatus status) {
    if (status == null) {
      return null;
    }

    switch (status) {
      case HEALTHY:
        return HopEncodingStatus.PARITY_HEALTHY;
      case REPAIR_REQUESTED:
        return HopEncodingStatus.PARITY_REPAIR_REQUESTED;
      case REPAIR_ACTIVE:
        return HopEncodingStatus.PARITY_REPAIR_ACTIVE;
      case REPAIR_CANCELED:
        return HopEncodingStatus.PARITY_REPAIR_CANCELED;
      case REPAIR_FAILED:
        return  HopEncodingStatus.PARITY_REPAIR_FAILED;
      default:
        throw new UnsupportedOperationException("Trying to convert an unknown status");
    }
  }

  @Override
  public void add(EncodingStatus status) throws StorageException {
    dataAccess.add(convertHDFStoDAL(status));
  }

  @Override
  public void update(EncodingStatus status) throws StorageException {
    dataAccess.update(convertHDFStoDAL(status));
  }

  @Override
  public void delete(EncodingStatus status) throws StorageException {
    dataAccess.delete(convertHDFStoDAL(status));
  }

  @Override
  public EncodingStatus findByInodeId(int inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByInodeId(inodeId));
  }

  @Override
  public EncodingStatus findByParityInodeId(int inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByParityInodeId(inodeId));
  }

  @Override
  public Collection<EncodingStatus> findRequestedEncodings(long limit) throws StorageException {
    return convertDALtoHDFS(dataAccess.findRequestedEncodings(limit));
  }

  @Override
  public Collection<EncodingStatus> findRequestedRepairs(long limit) throws StorageException {
    return convertDALtoHDFS(dataAccess.findRequestedRepairs(limit));
  }

  @Override
  public int countRequestedRepairs() throws StorageException {
    return dataAccess.countRequestedRepairs();
  }

  @Override
  public int countRequestedEncodings() throws StorageException {
    return dataAccess.countRequestedEncodings();
  }

  @Override
  public Collection<EncodingStatus> findActiveEncodings() throws StorageException {
    return convertDALtoHDFS(dataAccess.findActiveEncodings());
  }

  @Override
  public int countActiveEncodings() throws StorageException {
    return dataAccess.countActiveEncodings();
  }

  @Override
  public Collection<EncodingStatus> findEncoded(long limit) throws StorageException {
    return convertDALtoHDFS(dataAccess.findEncoded(limit));
  }

  @Override
  public int countEncoded() throws StorageException {
    return dataAccess.countEncoded();
  }

  @Override
  public Collection<EncodingStatus> findActiveRepairs() throws StorageException {
    return convertDALtoHDFS(dataAccess.findActiveRepairs());
  }

  @Override
  public int countActiveRepairs() throws StorageException {
    return dataAccess.countActiveRepairs();
  }

  @Override
  public Collection<EncodingStatus> findRequestedParityRepairs(long limit) throws StorageException {
    return convertDALtoHDFS(dataAccess.findRequestedParityRepairs(limit));
  }

  @Override
  public int countRequestedParityRepairs() throws StorageException {
    return dataAccess.countRequestedParityRepairs();
  }

  @Override
  public Collection<EncodingStatus> findActiveParityRepairs() throws StorageException {
    return convertDALtoHDFS(dataAccess.findActiveParityRepairs());
  }

  @Override
  public int countActiveParityRepairs() throws StorageException {
    return dataAccess.countActiveParityRepairs();
  }

  @Override
  public void setLostBlockCount(int n) {
    dataAccess.setLostBlockCount(n);
  }

  @Override
  public int getLostBlockCount() {
    return dataAccess.getLostBlockCount();
  }

  @Override
  public void setLostParityBlockCount(int n) {
    dataAccess.setLostParityBlockCount(n);
  }

  @Override
  public int getLostParityBlockCount() {
    return dataAccess.getLostParityBlockCount();
  }

  @Override
  public Collection<EncodingStatus> findDeleted(long limit) throws StorageException {
    return convertDALtoHDFS(dataAccess.findDeleted(limit));
  }

  @Override
  public Collection<EncodingStatus> findRevoked() throws StorageException {
    return convertDALtoHDFS(dataAccess.findRevoked());
  }
}
