package org.apache.doris.udf.util;

import java.nio.ByteBuffer;

/**
 * Utility class for generic round UDF.
 *
 */
public class ByteUtils {

  static final int[] bytesFromUTF8 = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5};
  static final int[] offsetsFromUTF8 = new int[]{0, 12416, 925824, 63447168, -100130688, -2113396608};

  private ByteUtils() {
  }

  public static int bytesToCodePoint(ByteBuffer bytes) {
    bytes.mark();
    byte b = bytes.get();
    bytes.reset();
    int extraBytesToRead = bytesFromUTF8[b & 255];
    if (extraBytesToRead < 0) {
      return -1;
    } else {
      int ch = 0;
      switch(extraBytesToRead) {
        case 5:
          ch += bytes.get() & 255;
          ch <<= 6;
        case 4:
          ch += bytes.get() & 255;
          ch <<= 6;
        case 3:
          ch += bytes.get() & 255;
          ch <<= 6;
        case 2:
          ch += bytes.get() & 255;
          ch <<= 6;
        case 1:
          ch += bytes.get() & 255;
          ch <<= 6;
        case 0:
          ch += bytes.get() & 255;
        default:
          ch -= offsetsFromUTF8[extraBytesToRead];
          return ch;
      }
    }
  }

}
