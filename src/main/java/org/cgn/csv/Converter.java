package org.cgn.csv;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Converter {
  private static String dbPrefix = null;
  private static String dbAllPrefix = null;
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
  private static final Session session = new Session.Builder()
      .host("127.0.0.1")
      .port(6667)
      .username("root")
      .password("root")
      .version(Version.V_1_0)
      .build();

  public static void convert(File zipFile, String prefix, String allPrefix) throws ParseException, IOException, IoTDBConnectionException, StatementExecutionException {
    dbPrefix = prefix;
    dbAllPrefix = allPrefix;

    session.open(false);
    session.setFetchSize(10000);

    // Convert zipped csv file to TsFile
    long timestamp = extractTimestamp(zipFile);
    writeTsFile(timestamp, zipFile);
  }

  private static void writeTsFile(long timestamp, File zipFile) throws IOException {
    List<String[]> values = new ArrayList<>();
    List<MeasurementSchema> schemas = new ArrayList<>();
    BufferedReader bufferedReader;

    String tsfileName = zipFile.getName() + ".tsfile";
    File tsfile = new File(tsfileName);
    try (TsFileWriter tsFileWriter = new TsFileWriter(tsfile)) {
      // Actually each zip file only contains one csv file
      InputStream stream = Files.newInputStream(Paths.get(zipFile.getAbsolutePath()));
      try (ZipInputStream zipInputStream = new ZipInputStream(stream)) {
        ZipEntry zipEntry;
        while ((zipEntry = zipInputStream.getNextEntry()) != null) {
          if (zipEntry.getName().endsWith(".csv")) {
            bufferedReader = new BufferedReader(new InputStreamReader(zipInputStream));

            int count = 0;
            String line;
            while ((line = bufferedReader.readLine()) != null) {
              // Each line is a new measurement
              MeasurementSchema measurementSchema = new MeasurementSchema("s_" + count, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.ZSTD);
              schemas.add(measurementSchema);

              // Each column stands for different timestamp
              String[] item = line.split(",");
              values.add(item);

              count++;
            }
          }
        }

        // Prepare schemas
        tsFileWriter.registerTimeseries(new Path(dbPrefix), schemas);
        Tablet tablet = new Tablet(dbPrefix, schemas, 200);

        // Prepare data
        long[] timestamps = tablet.timestamps;
        Object[] tabletValues = tablet.values;

        // ms -> ns
        timestamp *= 1_000_000;
        // Iterator column, i.e. datapoint
        int datapointCount = values.get(0).length;
        int measurementCount = schemas.size();
        for (int i = 0; i < datapointCount; i++) {
          int row = tablet.rowSize++;

          timestamps[row] = timestamp;
          timestamp += 70_000_000;    // 70ms per data?

          // Iterator row, i.e. measurement
          for (int j = 0; j < measurementCount; j++) {
            int[] tabletValue = (int[]) tabletValues[j];
            // Locate measurement
            String[] strings = values.get(j);
            // Locate data
            tabletValue[row] = Integer.parseInt(strings[row]);
          }

          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            tsFileWriter.write(tablet);
            tsFileWriter.flushAllChunkGroups();
            tablet.reset();
          }
        }

        // For display
        if (dbAllPrefix != null) {
          transformToRow(zipFile.getName(), timestamps, tabletValues, datapointCount, measurementCount);
        }

        if (tablet.rowSize != 0) {
          tsFileWriter.write(tablet);
        }
        tablet.reset();
      } catch (WriteProcessException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      session.executeNonQueryStatement(String.format("load '%s' onSuccess=delete", tsfile.getAbsolutePath()));
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  // Transform (time, s1, s2, s3, ...) to (time, s_all)
  public static void transformToRow(String filename, long[] timestamps, Object[] objects, int datapointCount, int measurementCount) {
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s_value", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.ZSTD));

    File recordFile = new File(filename + ".T.tsfile");
    try (TsFileWriter recordFileWriter = new TsFileWriter(recordFile)) {
      recordFileWriter.registerTimeseries(new Path(dbAllPrefix), measurementSchemas);

      Tablet transpose = new Tablet(dbAllPrefix, measurementSchemas, measurementCount * datapointCount);
      long[] times = transpose.timestamps;
      int[] values = (int[]) transpose.values[0];

      int index = 0;
      long gap = 70_000_000 / measurementCount;
      for (int i = 0; i < datapointCount; i++) {
        for (int j = 0; j < measurementCount; j++) {
          if (j == 0) {
            times[index] = timestamps[i];
          } else {
            times[index] = times[index - 1] + gap;
          }

          values[index] = ((int[]) objects[j])[i];
          index++;
        }
      }

      transpose.rowSize = measurementCount * datapointCount;
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

  private static long extractTimestamp(File zipFile) throws ParseException {
    // Extract timestamp from zip file's name
    String zipFileName = zipFile.getName();
    String timeString = zipFileName
        .substring(0, zipFileName.length() - 4)
        .replace("_____", " ");

    return dateFormat.parse(timeString).getTime();
  }
}
