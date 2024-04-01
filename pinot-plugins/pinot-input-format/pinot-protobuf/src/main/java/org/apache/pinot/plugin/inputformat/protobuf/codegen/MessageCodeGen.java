package org.apache.pinot.plugin.inputformat.protobuf.codegen;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ProtobufInternalUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.plugin.inputformat.protobuf.ProtoBufCodeGenMessgeDecoder;


public class MessageCodeGen {

  public String codegen(ClassLoader protoMessageClsLoader, String protoClassName, Set<String> fieldsToRead)
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException,
             InstantiationException {
    Descriptors.Descriptor descriptor = getDescriptorForProtoClass(protoMessageClsLoader, protoClassName);
    HashMap<String, String> msgDecodeCode = generateMessageDeserializeCode(descriptor, fieldsToRead);
    return generateCode(descriptor, fieldsToRead, msgDecodeCode);
  }

  public String generateCode(Descriptors.Descriptor descriptor, Set<String> fieldsToRead, HashMap<String, String> msgDecodeCode)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    String fullyQualifiedMsgName = descriptor.getFullName();
    String fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(descriptor.getName(), true);

    StringBuilder code = new StringBuilder();
    code.append("package org.apache.pinot.plugin.inputformat.protobuf.decoder;\n");
    code.append(addImports(Set.of(descriptor.getFullName(),
        "org.apache.pinot.spi.data.readers.GenericRow",
        "java.util.ArrayList",
        "java.util.HashMap",
        "java.util.List",
        "java.util.Map")));
    code.append("\n");
    code.append(String.format("public class %s {\n", ProtoBufCodeGenMessgeDecoder.extractorClassName));
    int indent = 1;
    code.append(addIndent("public static GenericRow execute(byte[] from, GenericRow to) throws Exception {", indent));
    code.append(completeLine(String.format("Map<String, Object> msgMap = %s(%s.parseFrom(from))", getDecoderMethodName(fieldNameInCode), fullyQualifiedMsgName), ++indent));

    List<Descriptors.FieldDescriptor> allDesc = new ArrayList<>();
    if(fieldsToRead != null && !fieldsToRead.isEmpty()) {
      for(String fieldName: fieldsToRead) {
        allDesc.add(descriptor.findFieldByName(fieldName));
      }
    } else {
      allDesc = descriptor.getFields();
    }
    for (Descriptors.FieldDescriptor desc: allDesc) {
      // If empty values are returned here, it will get
      code.append(addIndent(String.format("if(msgMap.containsKey(\"%s\")) {", desc.getName()), indent));
      code.append(completeLine(String.format("to.putValue(\"%s\", msgMap.get(\"%s\"))", desc.getName(), desc.getName()), ++indent));
      code.append(addIndent("}", --indent));
    }
    code.append(completeLine("return to", indent));
    code.append(addIndent("}", --indent));
    for (String msgCode: msgDecodeCode.values()) {
      code.append("\n");
      code.append(msgCode);
    }
    code.append(addIndent("}\n", --indent));
    return code.toString();
  }

  public String addImports(Set<String> classNames) {
    String code = "";
    for (String className: classNames) {
      code = code + "import " + className + ";\n";
    }
    return code;
  }


  private Descriptors.Descriptor getDescriptorForProtoClass(ClassLoader protoMessageClsLoader, String protoClassName)
      throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException,
             InstantiationException {
    Class<?> updateMessageClass = protoMessageClsLoader.loadClass(protoClassName);
    Object updateMessageInstance = updateMessageClass.getDeclaredConstructor().newInstance(); // Instantiate the class
    Method getDescriptorMethod = updateMessageClass.getMethod("getDescriptor");
    Descriptors.Descriptor descriptor = (Descriptors.Descriptor) getDescriptorMethod.invoke(updateMessageInstance);
    //Class<? extends Message>  updateMessage = (Class<Message>) protoMessageClsLoader.loadClass(protoClassName);
//    Descriptors.Descriptor descriptor = (Descriptors.Descriptor) updateMessage.getMethod("getDescriptor").invoke(null);
    return descriptor;
  }

  public HashMap<String, String> generateMessageDeserializeCode(Descriptors.Descriptor mainDescriptor, Set<String> fieldsToRead) {
    HashMap<String, String> msgDecodeCode = new HashMap<>();
    List<Descriptors.Descriptor> messagesToGenCodeFor = new LinkedList<>();
    messagesToGenCodeFor.add(mainDescriptor);

    ListIterator<Descriptors.Descriptor> iterator = messagesToGenCodeFor.listIterator();
    generateDecodeCodeForAMessage(msgDecodeCode, iterator, fieldsToRead);

    while (iterator.hasNext()) {
      generateDecodeCodeForAMessage(msgDecodeCode, iterator, new HashSet<>());
    }
    return msgDecodeCode;
  }

  private void generateDecodeCodeForAMessage(HashMap<String, String> msgDecodeCode, ListIterator<Descriptors.Descriptor> iterator, Set<String> fieldsToRead) {
    Descriptors.Descriptor descriptor = iterator.next();
    String fullyQualifiedMsgName = descriptor.getFullName();
    int varNum = 1;
    if(msgDecodeCode.containsKey(fullyQualifiedMsgName)) {
      return;
    }
    String msgInGenFuncName = ProtobufInternalUtils.underScoreToCamelCase(descriptor.getName(), true);
    StringBuilder code = new StringBuilder();
    int indent = 1;
    code.append(addIndent(
        String.format("private static Map<String, Object> %s(%s msg) {", getDecoderMethodName(msgInGenFuncName),
            fullyQualifiedMsgName), indent));
    code.append(completeLine("Map<String, Object> msgMap = new HashMap<>()", ++indent));
    List<Descriptors.FieldDescriptor> descriptorsToDerive = new ArrayList<>();
    if(fieldsToRead != null && !fieldsToRead.isEmpty()) {
      for(String fieldName: fieldsToRead) {
        descriptorsToDerive.add(descriptor.findFieldByName(fieldName));
      }
    } else {
      descriptorsToDerive = descriptor.getFields();
    }

    for (Descriptors.FieldDescriptor desc : descriptorsToDerive) {
      Descriptors.FieldDescriptor.Type type = desc.getType();
      String fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(desc.getName(), true);
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
          code.append(codeForScalarFieldExtraction(desc, fieldNameInCode, indent));
          break;
        case BYTES:
          code.append(codeForComplexFieldExtraction(desc, fieldNameInCode, indent, varNum, "", ".toByteArray()"));
          break;
        case ENUM:
          code.append(codeForComplexFieldExtraction(desc, fieldNameInCode, indent, varNum, "", ".getName()"));
          break;
        case MESSAGE:
          if(!msgDecodeCode.containsKey(desc.getFullName())) {
            iterator.add(desc.getMessageType());
          }
          code.append(codeForComplexFieldExtraction(desc, fieldNameInCode, indent, varNum, getDecoderMethodName(fieldNameInCode), ""));
          break;
        default:
          break;
      }
    }
    code.append(completeLine("return msgMap", indent));
    code.append(addIndent("}", --indent));
    msgDecodeCode.put(fullyQualifiedMsgName, code.toString());
  }

  private StringBuilder codeForScalarFieldExtraction(Descriptors.FieldDescriptor desc, String fieldNameInCode, int indent) {
    StringBuilder code = new StringBuilder();
    if (desc.isRepeated()) {
      code.append(addIndent(String.format("if (msg.%s < 1) {", getCountMethodName(fieldNameInCode)), indent));
      code.append(completeLine(putFieldInMsgMapCode(desc.getName(), getProtoFieldListMethodName(fieldNameInCode)), ++indent));
      code.append(addIndent("}", --indent));
    } else if (desc.hasPresence()) {
      code.append(addIndent(String.format("if (msg.%s) {", hasPresenceMethodName(fieldNameInCode)), indent));
      code.append(completeLine(putFieldInMsgMapCode(desc.getName(), getProtoFieldMethodName(fieldNameInCode)), ++indent));
      code.append(addIndent("}", --indent));
    } else {
      code.append(completeLine(putFieldInMsgMapCode(desc.getName(), getProtoFieldMethodName(fieldNameInCode)), indent));
    }
    return code;
  }

  private StringBuilder codeForComplexFieldExtraction(Descriptors.FieldDescriptor desc, String fieldNameInCode, int indent, int varNum, String decoderMethod, String additionalExtractions) {
    StringBuilder code = new StringBuilder();
    if(StringUtils.isBlank(additionalExtractions)) {
      additionalExtractions = "";
    }
    if (desc.isRepeated()) {
      varNum++;
      String listVarName = "list" + varNum;
      code.append(completeLine(String.format("List<Map<String, Object>> %s = new ArrayList<>()", listVarName), indent));
      code.append(addIndent(String.format("for (%s row: msg.%s) {", desc.getMessageType().getFullName(), getProtoFieldListMethodName(fieldNameInCode)), indent));
      if (!StringUtils.isBlank(decoderMethod)) {
        code.append(completeLine(String.format("%s.add(%s(row%s))", listVarName, decoderMethod, additionalExtractions), ++indent));
      } else {
        code.append(completeLine(String.format("%s.add(row%s)", listVarName, additionalExtractions), ++indent));
      }
      code.append(addIndent("}", --indent));
      code.append(addIndent(String.format("if (!%s.isEmpty()) {", listVarName), indent));
      code.append(completeLine(String.format("msgMap.put(\"%s\", %s.toArray())", desc.getName(), listVarName), ++indent));
      code.append(addIndent("}", --indent));
    } else if (desc.hasPresence()) {
      code.append(addIndent(String.format("if (msg.%s) {", hasPresenceMethodName(fieldNameInCode)), indent));
      code.append(completeLine(putFieldInMsgMapCode(desc.getName(), getProtoFieldMethodName(fieldNameInCode), decoderMethod, additionalExtractions), ++indent));
      code.append(addIndent("}", indent--));
    } else {
      code.append(completeLine(putFieldInMsgMapCode(desc.getName(), getProtoFieldMethodName(fieldNameInCode), decoderMethod,additionalExtractions), indent));
    }
    return code;
  }

  private String getDecoderMethodName(String msgNameInCode) {
    return String.format("decode%sMessage", msgNameInCode);
  }

  private String getProtoFieldMethodName(String msgNameInCode) {
    return String.format("get%s", msgNameInCode);
  }

  private String getProtoFieldListMethodName(String msgNameInCode) {
    return String.format("get%sList", msgNameInCode);
  }

  private String hasPresenceMethodName(String msgNameInCode) {
    return String.format("has%s", msgNameInCode);
  }

  private String getCountMethodName(String msgNameInCode) {
    return String.format("get%sCount", msgNameInCode);
  }

  private String putFieldInMsgMapCode(String fieldNameInProto, String fieldNameInCode) {
    return String.format("msgMap.put(\"%s\", msg.%s())", fieldNameInProto, fieldNameInCode);
  }

  private String putFieldInMsgMapCode(String fieldNameInProto, String getFieldMethodName, String optionalDecodeMethod, String optionalAdditionalCalls) {
    if(StringUtils.isBlank(optionalAdditionalCalls)) {
      optionalAdditionalCalls = "";
    }
    if (!StringUtils.isBlank(optionalDecodeMethod)) {
      return String.format("msgMap.put(\"%s\", %s(msg.%s()%s))", fieldNameInProto, optionalDecodeMethod, getFieldMethodName, optionalDecodeMethod);
    }
    return String.format("msgMap.put(\"%s\", msg.%s()%s)", fieldNameInProto, getFieldMethodName, optionalAdditionalCalls);
  }



  protected String completeLine(String line, int indent) {
    StringBuilder sb = new StringBuilder();
    sb.append(" ".repeat(Math.max(0, indent)));
    sb.append(line);
    sb.append(";\n");
    return sb.toString();
  }

  protected String addIndent(String line, int indent) {
    StringBuilder sb = new StringBuilder();
    sb.append(" ".repeat(Math.max(0, indent)));
    sb.append(line);
    sb.append("\n");
    return sb.toString();
  }
}
