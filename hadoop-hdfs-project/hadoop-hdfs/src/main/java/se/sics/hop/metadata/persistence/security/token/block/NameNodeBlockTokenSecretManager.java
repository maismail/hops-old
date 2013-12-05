package se.sics.hop.metadata.persistence.security.token.block;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import se.sics.hop.metadata.persistence.lock.TransactionLockAcquirer;
import se.sics.hop.metadata.persistence.lock.TransactionLockTypes;
import se.sics.hop.metadata.persistence.lock.TransactionLockTypes.LockType;
import se.sics.hop.metadata.persistence.lock.TransactionLocks;
import se.sics.hop.transcation.EntityManager;
import se.sics.hop.transcation.LightWeightRequestHandler;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import se.sics.hop.transcation.RequestHandler;
import se.sics.hop.transcation.TransactionalRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockTokenKeyDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.hadoop.util.Time;

/**
 * Persisted version of the BlockTokenSecretManager to be used by the NameNode
 * We add persistence by overriding only the methods used by the NameNode
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class NameNodeBlockTokenSecretManager extends BlockTokenSecretManager {

  private boolean isLeader;

  /**
   * Constructor for masters.
   */
  public NameNodeBlockTokenSecretManager(long keyUpdateInterval,
          long tokenLifetime, boolean isLeader, String blockPoolId,
          String encryptionAlgorithm) throws IOException {
    super(true, keyUpdateInterval, tokenLifetime, blockPoolId,
            encryptionAlgorithm);
    this.isLeader = isLeader;
    this.setSerialNo(new SecureRandom().nextInt());
    if (isLeader) {
      // TODO[Hooman]: Since Master is keeping the serialNo locally, so whenever
      // A namenode crashes it should remove all keys from the database.
      this.generateKeys();
    } else {
      retrieveBlockKeys();
    }
  }

  @Override
  public void setSerialNo(int serialNo) {
    this.serialNo = serialNo;
  }

  private void generateKeys() throws IOException {
    if (!isMaster) {
      return;
    }
    /*
     * Need to set estimated expiry dates for currentKey and nextKey so that if
     * NN crashes, DN can still expire those keys. NN will stop using the newly
     * generated currentKey after the first keyUpdateInterval, however it may
     * still be used by DN and Balancer to generate new tokens before they get a
     * chance to sync their keys with NN. Since we require keyUpdInterval to be
     * long enough so that all live DN's and Balancer will sync their keys with
     * NN at least once during the period, the estimated expiry date for
     * currentKey is set to now() + 2 * keyUpdateInterval + tokenLifetime.
     * Similarly, the estimated expiry date for nextKey is one keyUpdateInterval
     * more.
     */
    setSerialNo(serialNo + 1);
    currentKey = new BlockKey(serialNo, Time.now() + 2
            * keyUpdateInterval + tokenLifetime, generateSecret());
    currentKey.setKeyType(BlockKey.CURR_KEY);
    setSerialNo(serialNo + 1);
    nextKey = new BlockKey(serialNo, Time.now() + 3
            * keyUpdateInterval + tokenLifetime, generateSecret());
    nextKey.setKeyType(BlockKey.NEXT_KEY);
    addBlockKeys();
  }

  @Override
  public ExportedBlockKeys exportKeys() throws IOException {
    if (!isMaster) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exporting access keys");
    }
    BlockKey[] allKeys = getAllKeysAndSync();
    return new ExportedBlockKeys(true, keyUpdateInterval, tokenLifetime,
            currentKey, allKeys);

  }

  @Override
  public boolean updateKeys(final long updateTime) throws IOException {
    if (updateTime > keyUpdateInterval) {
      return updateKeys();
    }
    return false;
  }

  @Override
  boolean updateKeys() throws IOException {
    if (!isMaster) {
      return false;
    }
    if (isLeader) {
      LOG.info("Updating block keys");
      removeExpiredKeys();
      updateBlockKeys();
    } else {
      retrieveBlockKeys();
    }
    return true;
  }

  @Override
  public DataEncryptionKey generateDataEncryptionKey() throws IOException {
    byte[] nonce = new byte[8];
    nonceGenerator.nextBytes(nonce);
    BlockKey key = getBlockKeyByType(BlockKey.CURR_KEY);

    byte[] encryptionKey = createPassword(nonce, key.getKey());
    return new DataEncryptionKey(key.getKeyId(), blockPoolId, nonce,
            encryptionKey, Time.now() + tokenLifetime,
            encryptionAlgorithm);
  }

  @Override
  protected byte[] createPassword(BlockTokenIdentifier identifier) {
    BlockKey key;
    try {
      key = getBlockKeyByType(BlockKey.CURR_KEY);
    } catch (IOException ex) {
      throw new IllegalStateException("currentKey hasn't been initialized. [" + ex.getMessage() + "]");
    }
    if (key == null) {
      throw new IllegalStateException("currentKey hasn't been initialized.");
    }
    identifier.setExpiryDate(Time.now() + tokenLifetime);
    identifier.setKeyId(key.getKeyId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generating block token for " + identifier.toString());
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }

  @Override
  public byte[] retrievePassword(BlockTokenIdentifier identifier) throws InvalidToken {
    if (isExpired(identifier.getExpiryDate())) {
      throw new InvalidToken("Block token with " + identifier.toString()
              + " is expired.");
    }
    BlockKey key = null;
    try {
      key = getBlockKeyById(identifier.getKeyId());
    } catch (IOException ex) {
    }

    if (key == null) {
      throw new InvalidToken("Can't re-compute password for "
              + identifier.toString() + ", since the required block key (keyID="
              + identifier.getKeyId() + ") doesn't exist.");
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }

  public void generateKeysIfNeeded(boolean isLeader) throws IOException{
    this.isLeader = isLeader;
    retrieveBlockKeys();
    if(currentKey == null && nextKey == null){
      generateKeys();
    }
  }
  
  public void updateLeaderState(boolean isLeader) {
    this.isLeader = isLeader;
  }

  private void retrieveBlockKeys() throws IOException {
    currentKey = getBlockKeyByType(BlockKey.CURR_KEY);
    nextKey = getBlockKeyByType(BlockKey.NEXT_KEY);
  }

  private void addBlockKeys() throws IOException {
    new TransactionalRequestHandler(RequestHandler.OperationType.ADD_BLOCK_TOKENS) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        EntityManager.add(currentKey);
        EntityManager.add(nextKey);
        return null;
      }
    }.handle(null);
  }

  private BlockKey getBlockKeyById(int keyId) throws IOException {
    return getBlockKey(true, keyId);
  }

  private BlockKey getBlockKeyByType(short keytype) throws IOException {
    return getBlockKey(false, keytype);
  }

  private BlockKey getBlockKey(final boolean byID, final int keyIdOrType) throws IOException {
    return (BlockKey) new LightWeightRequestHandler(byID ? RequestHandler.OperationType.GET_KEY_BY_ID : RequestHandler.OperationType.GET_KEY_BY_TYPE) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        BlockTokenKeyDataAccess da = (BlockTokenKeyDataAccess) StorageFactory.getDataAccess(BlockTokenKeyDataAccess.class);
        if (byID) {
          return da.findByKeyId(keyIdOrType);
        } else {
          return da.findByKeyType((short) keyIdOrType);
        }
      }
    }.handle(null);
  }

  private BlockKey[] getAllKeysAndSync() throws IOException {
    BlockKey[] allKeysArr = null;
    List<BlockKey> allKeys = getAllKeys();
    if (allKeys != null) {
      for (BlockKey key : allKeys) {
        if (key.isCurrKey()) {
          currentKey = key;
        } else if (key.isNextKey()) {
          nextKey = key;
        }
      }
      allKeysArr = allKeys.toArray(new BlockKey[allKeys.size()]);
    }
    return allKeysArr;
  }

  private List<BlockKey> getAllKeys() throws IOException {
    return (List<BlockKey>) new LightWeightRequestHandler(RequestHandler.OperationType.REMOVE_ALL_BLOCK_KEYS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        BlockTokenKeyDataAccess da = (BlockTokenKeyDataAccess) StorageFactory.getDataAccess(BlockTokenKeyDataAccess.class);
        return da.findAll();
      }
    }.handle(null);
  }

  private void removeExpiredKeys() throws IOException {
    TransactionalRequestHandler removeKeyHandler = new TransactionalRequestHandler(RequestHandler.OperationType.REMOVE_BLOCK_KEY) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        int keyId = (Integer) params[0];
        TransactionLockAcquirer tla = new TransactionLockAcquirer();
        tla.getLocks().addBlockKeyLockById(TransactionLockTypes.LockType.WRITE, keyId);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        int keyId = (Integer) params[0];
        BlockKey key = EntityManager.find(BlockKey.Finder.ById, keyId);
        if (key != null) {
          if (isExpired(key.getExpiryDate())) {
            EntityManager.remove(key);
          }
        }
        return null;
      }
    };

    List<BlockKey> allKeys = getAllKeys();
    for (BlockKey key : allKeys) {
      removeKeyHandler.setParams(key.getKeyId());
      removeKeyHandler.handle(null);
    }
  }

  private void updateBlockKeys() throws IOException {
    new TransactionalRequestHandler(RequestHandler.OperationType.UPDATE_BLOCK_KEYS) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        TransactionLockAcquirer tla = new TransactionLockAcquirer();
        tla.getLocks().
                addBlockKeyLockByType(LockType.WRITE, BlockKey.CURR_KEY).
                addBlockKeyLockByType(LockType.WRITE, BlockKey.NEXT_KEY);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        // set final expiry date of retiring currentKey
        // also modifying this key to mark it as 'simple key' instead of 'current key'
        BlockKey currentKeyFromDB = EntityManager.find(BlockKey.Finder.ByType, BlockKey.CURR_KEY);
        currentKeyFromDB.setExpiryDate(Time.now() + keyUpdateInterval + tokenLifetime);
        currentKeyFromDB.setKeyType(BlockKey.SIMPLE_KEY);
        EntityManager.update(currentKeyFromDB);
        // after above update, we only have a key marked as 'next key'
        // the 'next key' becomes the 'current key'
        // update the estimated expiry date of new currentKey
        BlockKey nextKeyFromDB = EntityManager.find(BlockKey.Finder.ByType, BlockKey.NEXT_KEY);
        currentKey = new BlockKey(nextKeyFromDB.getKeyId(), Time.now()
                + 2 * keyUpdateInterval + tokenLifetime, nextKeyFromDB.getKey());
        currentKey.setKeyType(BlockKey.CURR_KEY);
        EntityManager.update(currentKey);

        // generate a new nextKey
        setSerialNo(serialNo + 1);
        nextKey = new BlockKey(serialNo, Time.now() + 3
                * keyUpdateInterval + tokenLifetime, generateSecret());
        nextKey.setKeyType(BlockKey.NEXT_KEY);
        EntityManager.add(nextKey);
        return null;
      }
    }.handle(null);
  }
}
