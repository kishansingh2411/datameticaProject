package com.clickstream.avroserializer;

public class CorruptedRecordException extends RuntimeException {

  /**
   * Default Serial Number
   */
  private static final long serialVersionUID = 1L;

  /**
   * Standard Contructor
   * 
   * @param message
   * @param e
   */
  public CorruptedRecordException(String message, Throwable e) {
    super(message, e);
  }

  /**
   * Standard Contructor
   * 
   * @param message
   */
  public CorruptedRecordException(String message) {
    super(message);
  }

}
