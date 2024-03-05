package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProtoBufCodeGenTestMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufCodeGenTestMessageDecoder.class);

  public static final String PROTOBUF_JAR_FILE_PATH = "descriptorFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";

  private ProtobufRecorderUpdateMessageExtractor _recordExtractor;
  private Message.Builder _builder;
  private Set<String> _fields;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(PROTOBUF_JAR_FILE_PATH),
        "Protocol Buffer schema jar file must be provided");
    String protoClassName = props.getOrDefault(PROTO_CLASS_NAME, "");
    String jarPath = props.getOrDefault(PROTOBUF_JAR_FILE_PATH, "");
    Class<?> cls = loadProtobufClass(jarPath, protoClassName);

    try {
      // Check if the loaded class is a protobuf-generated class
      Class<? extends GeneratedMessageV3> messageClass = (Class<? extends GeneratedMessageV3>) cls;
      java.lang.reflect.Method builderMethod = messageClass.getMethod("newBuilder");
      Object builder = builderMethod.invoke(null);
      _builder = (Message.Builder) builder;
    } catch (Exception e) {
      e.printStackTrace();
    }
    _fields = fieldsToRead;
    _recordExtractor = new ProtobufRecorderUpdateMessageExtractor();
    _recordExtractor.init(fieldsToRead, null);
  }

  private static Class<?> loadProtobufClass(String jarFilePath, String className) {
    try {
      File file = ProtoBufUtils.getFileCopiedToLocal(jarFilePath);
      URL url = file.toURI().toURL();
      URL[] urls = new URL[]{url};
      ClassLoader cl = new URLClassLoader(urls);
      return cl.loadClass(className);
    } catch (Exception e) {
      throw new RuntimeException("Error loading protobuf class", e);
    }
  }

  @Nullable
  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    _recordExtractor.extract(payload, destination);
    return destination;
  }

  @Nullable
  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
