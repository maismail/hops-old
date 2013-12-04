package se.sics.hop.metadata.persistence.context;

import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author salman niazi <salman@sics.se>
 */
public class TransactionLockAcquireFailure extends StorageException {

  public TransactionLockAcquireFailure(String msg) {
    super(msg);
  }
  
}
