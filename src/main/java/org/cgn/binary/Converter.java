package org.cgn.binary;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class Converter {
  private static String dbPrefix = null;
  private static String dbAllPrefix = null;
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd__HH_mm_ss");
  private static final int sensorCount = 4096; // suppose we have 4096 sensors
  private static final int BUFFER_SIZE = 4096 * 4 * 10000; // read 10000 rows at a time
  private static final Session session = new Session.Builder()
      .host("127.0.0.1")
      .port(6667)
      .username("root")
      .password("root")
      .version(Version.V_1_0)
      .build();

  public static void convert(File binFile, String prefix, String allPrefix) throws ParseException, IOException, IoTDBConnectionException, StatementExecutionException {
    dbPrefix = prefix;
    dbAllPrefix = allPrefix;

    session.open(false);
    session.setFetchSize(10000);

    // Convert binary file to TsFile
    long timestamp = extractTimestamp(binFile);
    writeTsFile(timestamp, binFile);
  }

  private static void writeTsFile(long timestamp, File binFile) throws IOException {
    // Prepare schemas
    List<MeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < sensorCount; i++) {
      schemas.add(new MeasurementSchema("s_" + i + "_amp", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZMA2));
      schemas.add(new MeasurementSchema("s_" + i + "_phase", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZMA2));
    }

    String tsfileName = binFile.getName() + ".tsfile";
    File tsfile = new File(tsfileName);
    try (TsFileWriter tsFileWriter = new TsFileWriter(tsfile);
         FileInputStream inputStream = new FileInputStream(binFile)) {
      tsFileWriter.registerTimeseries(new Path(dbPrefix), schemas);

      int bytesRead;
      byte[] buffer = new byte[BUFFER_SIZE];
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        // Process 10000 rows at a time
        Tablet tablet = new Tablet(dbPrefix, schemas, 10000);

        // Prepare timestamps
        long[] timestamps = tablet.timestamps;
        Object[] tabletValues = tablet.values;
        for (int i = 0; i < 10000; i++) {
          timestamps[i] = timestamp;
          timestamp += 65_000_000;
        }

        // Prepare data
        List<List<Integer>> values = processBuffer(buffer, bytesRead);
        for (int i = 0; i < values.get(0).size(); i++) {
          int[] tabletValue = (int[]) tabletValues[i];
          for (int j = 0; j < values.size(); j++) {
            tabletValue[j] = values.get(j).get(i);
          }
        }

        // For display
        if (dbAllPrefix != null) {
          transformToRow(binFile.getName(), timestamps, values, 10000, 4096);
        }

        tablet.rowSize = 10000;
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          tsFileWriter.write(tablet);
          tsFileWriter.flushAllChunkGroups();
          tablet.reset();
        }
      }
    } catch (WriteProcessException e) {
      throw new RuntimeException(e);
    }

    try {
      session.executeNonQueryStatement(String.format("load '%s' onSuccess=delete", tsfile.getAbsolutePath()));
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  // Transform (time, s_1_phase, s_1_amp, s_2_phase, ...) to (time, s_amp, s_phase)
  public static void transformToRow(String filename, long[] timestamps, List<List<Integer>> values, int datapointCount, int measurementCount) {
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s_amp", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZMA2));
    measurementSchemas.add(new MeasurementSchema("s_phase", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZMA2));


    File recordFile = new File(filename + ".T.tsfile");
    try (TsFileWriter recordFileWriter = new TsFileWriter(recordFile)) {
      recordFileWriter.registerTimeseries(new Path(dbAllPrefix), measurementSchemas);

      Tablet transpose = new Tablet(dbAllPrefix, measurementSchemas, datapointCount * measurementCount);
      long[] times = transpose.timestamps;
      int[] sensor0 = (int[]) transpose.values[0];
      int[] sensor1 = (int[]) transpose.values[1];

      long gap = 65_000_000 / measurementCount;
      int index = 0;
      for (int i = 0; i < datapointCount; i++) {
        for (int j = 0; j < measurementCount; j++) {
          if (j == 0) {
            times[index] = timestamps[i];
          } else {
            times[index] = times[index - 1] + gap;
          }

          sensor0[index] = values.get(i).get(2 * j);
          sensor1[index] = values.get(i).get(2 * j + 1);

          index++;
        }
      }

      transpose.rowSize = datapointCount * measurementCount;
      recordFileWriter.write(transpose);
      recordFileWriter.flushAllChunkGroups();
      transpose.reset();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      session.executeNonQueryStatement(String.format("load '%s' onSuccess=delete", recordFile.getAbsolutePath()));
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<List<Integer>> processBuffer(byte[] buffer, int bytesRead) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

    int count = 0;
    List<List<Integer>> values = new ArrayList<>();
    List<Integer> senValues = new ArrayList<>();
    // Handle 4 bytes at a time
    while (byteBuffer.remaining() >= 4) {
      int value = byteBuffer.getInt();
      // Extract high 16 bits and low 16 bits
      short high16 = (short) ((value >> 16) & 0xFFFF);
      short low16 = (short) (value & 0xFFFF);
      senValues.add((int) high16);
      senValues.add((int) low16);

      if (count == 4095) {
        values.add(senValues);
        senValues = new ArrayList<>();
        count = 0;
        continue;
      }

      count++;
    }

    return values;
  }

  private static long extractTimestamp(File binFile) throws ParseException {
    // Extract timestamp from zip file's name
    String binFileName = binFile.getName();
    String timeString = binFileName.substring(4);

    return dateFormat.parse(timeString).getTime();
  }
}
