package com.examples.beam.writer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;


/**
 * Unit test for TextIO writer
 */
@RunWith(JUnitParamsRunner.class)
public class TestTextIOWriter {
  @Rule
  public transient TestPipeline testPipeline = TestPipeline.create();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Test writing data to a file without sharding
   *
   * @param lines file content
   */
  @Test
  @Parameters
  public void testWriter(String[] lines) {
    String outputFilePath = temporaryFolder.getRoot() + "/output.txt";

    // write to file
    testPipeline
        .apply(Create.of(Arrays.asList(lines)).withCoder(StringUtf8Coder.of()))
        .apply(TextIOWriter.getWriter(outputFilePath));
    testPipeline.run();

    // validate output file
    fileContentAssertion(outputFilePath, lines);
  }

  /**
   * Test case parameter provider
   *
   * @return array of file contents
   */
  private Object parametersForTestWriter() {
    return new Object[]{
        new Object[]{new String[]{"line 1", "line 2", "line 3"}}
    };
  }

  /**
   * Validate output file content
   *
   * @param filePath file path to be validated
   * @param expected expected file content
   */
  private void fileContentAssertion(String filePath, String[] expected) {
    List<String> actual = new ArrayList<>();
    try (
        FileReader fr = new FileReader(new File(filePath));
        BufferedReader br = new BufferedReader(fr)) {
      String line;
      while ((line = br.readLine()) != null) {
        actual.add(line);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    assertThat(actual, containsInAnyOrder(expected));
  }
}
