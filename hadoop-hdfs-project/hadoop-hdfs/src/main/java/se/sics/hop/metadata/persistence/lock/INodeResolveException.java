package se.sics.hop.metadata.persistence.lock;

import se.sics.hop.metadata.persistence.exceptions.PersistanceException;

/**
 *
 * @author hooman
 */
public class INodeResolveException extends PersistanceException {

  public INodeResolveException(String message) {
    super(message);
  }

  public INodeResolveException(Throwable ex) {
    super(ex);
  }
}
