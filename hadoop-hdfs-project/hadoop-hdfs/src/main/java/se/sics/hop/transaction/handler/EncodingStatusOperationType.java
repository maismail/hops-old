package se.sics.hop.transaction.handler;

public enum EncodingStatusOperationType implements RequestHandler.OperationType {
  ADD,
  DELETE,
  UPDATE,
  FIND_BY_INODE_ID,
  FIND_ACTIVE_ENCODINGS,
  FIND_REQUESTED_ENCODINGS,
  FIND_ENCODED,
  FIND_ACTIVE_REPAIRS,
  COUNT_REQUESTED_ENCODINGS,
  COUNT_ACTIVE_ENCODINGS,
  COUNT_ENCODED,
  COUNT_ACTIVE_REPAIRS
}