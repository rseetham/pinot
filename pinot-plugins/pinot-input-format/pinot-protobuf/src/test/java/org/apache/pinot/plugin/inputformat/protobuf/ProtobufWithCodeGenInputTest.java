package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.protobuf.ProtobufWithCodeGenInput.CODE_GEN_FILE_PATH;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtobufWithCodeGenInput.EXTRACTOR_CLASS_NAME;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtobufWithCodeGenInput.PROTOBUF_JAR_FILE_PATH;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtobufWithCodeGenInput.PROTO_CLASS_NAME;


public class ProtobufWithCodeGenInputTest {

  @Test
  public void testHappyCase()
      throws Exception {

    Map<String, String> decoderProps = new HashMap<>();
    decoderProps.put(PROTOBUF_JAR_FILE_PATH,"/Users/rekhas/Projects/stuff/protobuf/UpdateMessageJar.jar");
    decoderProps.put(PROTO_CLASS_NAME,"com.uber.sia.common.models.generated.UpdateMessage");
    decoderProps.put(CODE_GEN_FILE_PATH,"/Users/rekhas/Projects/stuff/protobuf/UpdateMessageExtractorAll.txt");
    decoderProps.put(EXTRACTOR_CLASS_NAME,"org.apache.pinot.plugin.inputformat.protobuf.decoder.ProtobufRecorderUpdateMessageExtractor");
    URL descriptorFile = getClass().getClassLoader().getResource("sample.desc");
    decoderProps.put("descriptorFile", descriptorFile.toURI().toString());
    ProtoBufCodeGenMessgeDecoder messageDecoder = new ProtoBufCodeGenMessgeDecoder();
    /*Set<String> fieldsToRead = Set.of( "field", "terms",  "range_scan_enabled", "is_deleted",  "strings_positions_per_term",
        "doubles_positions_per_term", "ints_positions_per_term",  "longs_positions_per_term",  "bytes_positions_per_term", "floats_positions_per_term", "range_postings_list_enabled");
*/
    Set<String> fieldsToRead = Set.of("uuid", "term_updates", "stored_field_updates", "doc_values_updates", "bytes_uuid", "createdAt");
    List<byte[]> byteLines = decodeBase64LinesFromFile("/Users/rekhas/Projects/stuff/protobuf/100siastreammsgs");
    messageDecoder.init(decoderProps, fieldsToRead, "");
    GenericRow destination = new GenericRow();
    Long start = System.currentTimeMillis();
    //Long totalTime = 0L;
    int num = 0;
    for (int i = 0; i < 1; i++) {
      for (byte[] msg : byteLines) {
       // start = System.currentTimeMillis();
        messageDecoder.decode(msg, destination);
        //totalTime = totalTime + System.currentTimeMillis() - start;
        num = num + 1;
      }
    }
    System.out.println(System.currentTimeMillis() - start);
    System.out.println(num);
    //System.out.println(totalTime * 1.0/num);
    //assertNotNull(destination.getValue("uuid"));
    //assertNotNull(destination.getValue("term_updates"));
    //assertNotNull(destination.getValue("stored_field_updates"));
  }

  public static List<byte[]> decodeBase64LinesFromFile(String filePath) throws IOException {
    Path path = Paths.get(filePath);
    List<String> lines = Files.readAllLines(path);

    List<byte[]> decodedLines = new ArrayList<>();
    for (String line : lines) {
      byte[] decodedBytes = BaseEncoding.base64().decode(line);
      decodedLines.add(decodedBytes);
    }

    return decodedLines;
  }

}
