package org.cgn.binary;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class FileWatcher {
  private static String binaryPath = null;
  private static String dbPrefix = null;
  private static String dbAllPrefix = null;

  private static void parseArgs(String[] args) {
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--path":
          if (i + 1 < args.length) {
            binaryPath = args[i + 1];
            i++;
          } else {
            System.out.println("Error: --path requires a value");
          }
          break;

        case "--prefix":
          if (i + 1 < args.length) {
            dbPrefix = args[i + 1];
            i++;
          } else {
            System.out.println("Error: --prefix requires a value");
          }
          break;

        case "--all-prefix":
          if (i + 1 < args.length) {
            dbAllPrefix = args[i + 1];
            i++;
          }
          break;

        default:
          System.out.println("Unknown parameter: " + args[i]);
      }
    }
  }

  private static final Path LOG_FILE = Paths.get("processed_bin_files.log");
  private static final Object LOCK = new Object();

  public static void main(String[] args) {
    parseArgs(args);

    Path dir = Paths.get(binaryPath);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Set<String> processedFiles = new HashSet<>();

    loadProcessedFiles(processedFiles);
    try {
      // First scan existing files
      scanAndProcessExistingFiles(dir, processedFiles, executor);

      // Register watch service
      WatchService watchService = FileSystems.getDefault().newWatchService();
      dir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

      // Double scan
      scanAndProcessExistingFiles(dir, processedFiles, executor);

      while (true) {
        WatchKey key = watchService.take();
        for (WatchEvent<?> event : key.pollEvents()) {
          WatchEvent.Kind<?> kind = event.kind();
          if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
            Path fileName = (Path) event.context();
            Path filePath = dir.resolve(fileName);
            String filePathStr = filePath.toString();
            System.out.println("检测到新文件: " + filePathStr);

            if (processedFiles.add(filePathStr)) {
              executor.submit(() -> {
                processNewFile(filePath);
                recordProcessedFile(filePathStr);
              });
            }
          }
        }

        // Reset key to watch other events
        boolean valid = key.reset();
        if (!valid) {
          break;
        }
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      executor.shutdown();
    }
  }

  private static void scanAndProcessExistingFiles(Path dir, Set<String> processedFiles, ExecutorService executor) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      files.forEach(file -> {
        String filePathStr = file.toString();
        if (processedFiles.add(filePathStr)) {
          executor.submit(() -> {
            processNewFile(Paths.get(filePathStr));
            recordProcessedFile(filePathStr);
          });
        }
      });
    }
  }

  private static void processNewFile(Path fileName) {
    System.out.println("处理文件: " + fileName);
    try {
      Converter.convert(fileName.toFile(), dbPrefix, dbAllPrefix);
    } catch (ParseException | IOException | IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
    System.out.println("文件处理完成: " + fileName);
  }

  private static void loadProcessedFiles(Set<String> processedFiles) {
    if (Files.exists(LOG_FILE)) {
      try (Stream<String> lines = Files.lines(LOG_FILE, StandardCharsets.UTF_8)) {
        lines.forEach(processedFiles::add);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static void recordProcessedFile(String fileName) {
    synchronized (LOCK) {
      try {
        Files.write(LOG_FILE, (fileName + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}