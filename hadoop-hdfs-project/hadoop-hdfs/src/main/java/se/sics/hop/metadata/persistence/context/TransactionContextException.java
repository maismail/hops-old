package se.sics.hop.metadata.persistence.context;

import se.sics.hop.metadata.persistence.exceptions.PersistanceException;


/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class TransactionContextException extends PersistanceException {

  public TransactionContextException(String msg) {
    super(msg);
  }
  
}
