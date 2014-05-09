package se.sics.hop.metadata.adaptor;

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
  public HopEncodingStatus convertHDFStoDAL(EncodingStatus encodingStatus) throws StorageException {
    if (encodingStatus == null) {
      return null;
    }

    HopEncodingStatus converted = new HopEncodingStatus();
    converted.setInodeId(encodingStatus.getInodeId());
    converted.setCodec(encodingStatus.getCodec());
    converted.setStatus(convertStatus(encodingStatus.getStatus()));
    converted.setModification_time(encodingStatus.getModificationTime());
    return converted;
  }

  @Override
  public EncodingStatus convertDALtoHDFS(HopEncodingStatus hopEncodingStatus) throws StorageException {
    if (hopEncodingStatus == null) {
      return null;
    }

    EncodingStatus converted = new EncodingStatus();
    converted.setInodeId(hopEncodingStatus.getInodeId());
    converted.setCodec(hopEncodingStatus.getCodec());
    converted.setStatus(convertStatus(hopEncodingStatus.getStatus()));
    converted.setModificationTime(hopEncodingStatus.getModificationTime());
    return converted;
  }

  private EncodingStatus.Status convertStatus(int status) {
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
      default:
        throw new UnsupportedOperationException("Trying to convert an unknown status");
    }
  }

  private int convertStatus(EncodingStatus.Status status) {
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
  public EncodingStatus findByInodeId(long inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByInodeId(inodeId));
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
}
