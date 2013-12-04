package se.sics.hop.metadata.persistence.context;

import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author Salman <salman@sics.se>
 */
public class LockUpgradeException extends StorageException {

  public LockUpgradeException(String message) {
    super(message);
  }
  
  public LockUpgradeException(Throwable ex)
  {
    super(ex);
  }
}
