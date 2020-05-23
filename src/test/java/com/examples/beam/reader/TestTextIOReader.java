package com.examples.beam.reader;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Unit test for TextIO reader
 */
public class TestTextIOReader {
  @Rule
  public transient TestPipeline testPipeline = TestPipeline.create();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Create temporary file for unit test
   *
   * @param lines file content
   * @return file path
   * @throws IOException if fail to create file
   */
  private String createTemporaryFile(List<String> lines) {
    // Output array elements into file
    File file;
    try (
        FileWriter fw = new FileWriter(file = this.temporaryFolder.newFile());
        BufferedWriter bw = new BufferedWriter(fw)) {
      for (String line : lines) bw.write(line + "\n");
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    return file.getPath();
  }

  /**
   * Test reading a single file
   */
  @Test
  public void testReadSingleFile() {
    // create temporary file
    List<String> lines = Arrays.asList("L1", "L2", "L3");
    String filePath = createTemporaryFile(lines);

    // read file
    PCollection<String> pCollection = this.testPipeline
        .apply(TextIOReader.getReader(filePath));

    // validate
    PAssert.that(pCollection).containsInAnyOrder(lines);

    // execute
    this.testPipeline.run();
  }

  /**
   * Test reading multiple files with wildcard
   */
  @Test
  public void testReadMultipleFiles() {
    // dummy data
    List<String> lines1 = Arrays.asList("file1, L1", "file1, L2", "file1, L3");
    List<String> lines2 = Arrays.asList("file2, L1", "file2, L2", "file2, L3");

    // create temporary file
    createTemporaryFile(lines1);
    createTemporaryFile(lines2);

    // file pattern
    String filePattern = this.temporaryFolder.getRoot().getPath() + "/*";

    // read file
    PCollection<String> pCollection = this.testPipeline
        .apply(TextIOReader.getReader(filePattern));

    // validate
    List<String> joined = new ArrayList<>();
    joined.addAll(lines1);
    joined.addAll(lines2);
    PAssert.that(pCollection).containsInAnyOrder(joined);

    // execute
    this.testPipeline.run();
  }
}
