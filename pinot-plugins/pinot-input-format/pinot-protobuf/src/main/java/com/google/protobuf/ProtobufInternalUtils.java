package com.google.protobuf;

public class ProtobufInternalUtils {
  /** convert underscore name to camel name. */
  public static String underScoreToCamelCase(String name, boolean capNext) {
    return SchemaUtil.toCamelCase(name, capNext);
  }
}