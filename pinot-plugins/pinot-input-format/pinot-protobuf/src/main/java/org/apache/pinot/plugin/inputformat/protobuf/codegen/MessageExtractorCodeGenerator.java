/*
package org.apache.pinot.plugin.inputformat.protobuf.codegen;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;


public class MessageExtractorCodeGenerator {

  public HashMap<String, String> codegen(ClassLoader protoMessageClsLoader, String protoClassName, String fieldsToRead)
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    Descriptors.Descriptor descriptor = getDescriptorForProtoClass(protoMessageClsLoader, protoClassName);
    HashMap<String, String> msgDecodeCode = new MessageCodeGen().generateMessageDeserializeCode(descriptor);
    String codeGenCode = generateCode(protoMessageClsLoader, protoClassName, fieldsToRead);

  }

  private Descriptors.Descriptor getDescriptorForProtoClass(ClassLoader protoMessageClsLoader, String protoClassName)
      throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
    Class<com.google.protobuf.Message>  updateMessage = (Class<Message>) protoMessageClsLoader.loadClass(protoClassName);
    Descriptors.Descriptor descriptor = (Descriptors.Descriptor) updateMessage.getMethod("getDescriptor").invoke(null);
    return descriptor;
  }

   public String generateCode(ClassLoader protoMessageClsLoader, String protoClassName, Set<String> fieldsToRead)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Descriptors.Descriptor descriptor = getDescriptorForProtoClass(protoMessageClsLoader, protoClassName);
    String code = "package com.uber.uPinot.decoder;\n";
    code = code + addImports(Set.of(protoClassName,
        "org.apache.pinot.spi.data.readers.GenericRow",
        "java.util.ArrayList",
        "java.util.HashMap",
        "java.util.List",
        "java.util.Map"));

    code = code + "\n"
        + "public class ProtobufRecorderMessageExtractor {\n\n" //add random name
        + "  public static GenericRow execute(byte[] from, GenericRow to) throws Exception {\n"
        + String.format("    %s msg = %s.parseFrom(from);\n", protoClassName, protoClassName);

    Set<String> messageDecodeCode = new HashSet<>();

    for (String field: fieldsToRead) {
      FieldDescriptor desc = descriptor.findFieldByName(field);
      FieldDescriptor.Type type = desc.getType();
      String nameInCode = ProtobufInternalUtils.underScoreToCamelCase(desc.getName(), true);
      switch (type) {
        case STRING:
        case INT32:
        case INT64:
        case UINT64:
        case FIXED64:
        case FIXED32:
        case UINT32:
        case SFIXED64:
        case SINT32:
        case SINT64:
        case DOUBLE:
        case FLOAT:
        case BOOL:
          if(desc.isRepeated()) {
            code = code
                + String.format("    if (msg.get%sCount() > 0) {\n", nameInCode)
                + String.format("          to.putValue(\"%s\", msg.get%sList().toArray());\n", field, nameInCode)
                + "          }\n";
          } else {
            if (desc.hasPresence()) {
              code = code
                  + String.format("    if (msg.has%s()) {\n", nameInCode)
                  + String.format("          to.putValue(\"%s\", msg.get%s());\n", field, nameInCode)
                  + "          }\n";
            } else {
              code = code + String.format("    to.putValue(\"%s\", msg.get%s());\n", field, nameInCode);
            }
          }
          break;
        case BYTES:
          // TODO repeated bytes
          if (desc.hasPresence()) {
            code = code + String.format("    if (msg.has%s()) {\n", nameInCode)
                + String.format("          to.putValue(\"%s\", msg.get%s().toByteArray());\n", field, nameInCode)
                + "          }\n";
          } else {
            code = code + String.format("    to.putValue(\"%s\", msg.get%s().toByteArray());\n", field, nameInCode);
          }
          break;
        case MESSAGE:
          messageDecodeCode.add(new MessageCodeGen().generateMessageDeserializeCode(desc.getMessageType()));
          if (desc.hasPresence()) {
            code = code + String.format("    if (msg.has%s()) {\n", nameInCode)
                + String.format("          to.putValue(\"%s\", decode%sMessage(msg.get%s()));\n", field, nameInCode, nameInCode)
                + "          }\n";
          } else {
            code = code + String.format("    to.putValue(\"%s\", decode%sMessage(msg.get%s()));\n", field, nameInCode, nameInCode);
          }
        default:
          break;
      }
    }
    code = code + "    return to;\n";
    code = code + "  }\n";
    for (String msgCode: messageDecodeCode) {
      code = code + "\n";
      code = code + msgCode;
    }
    code = code + "}\n";
    return code;
  }

}
*/
