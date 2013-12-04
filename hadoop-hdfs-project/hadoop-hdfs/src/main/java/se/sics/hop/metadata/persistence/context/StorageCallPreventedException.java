package se.sics.hop.metadata.persistence.context;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public class StorageCallPreventedException extends TransactionContextException {
    public StorageCallPreventedException(String msg) {
    super(msg);
  }
}
