package com.examples.beam.writer;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;

/**
 * Sample code for TextIO writer
 */
public class TextIOWriter {
  /**
   * Interface for pipeline options
   */
  public interface CustomOptions extends PipelineOptions {
    @Validation.Required
    String getOutputFilePath();

    void setOutputFilePath(String s);
  }

  /**
   * Get TextIO writer
   *
   * @param outputFilePath Glob file pattern for input files
   * @return TextIO reader
   */
  static TextIO.Write getWriter(String outputFilePath) {
    return TextIO
        .write()
        .to(outputFilePath)
        // Do not allow splitting files
        .withoutSharding();
  }

  /**
   * Define pipeline graph and execute
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    CustomOptions opt = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(CustomOptions.class);
    Pipeline pipeline = Pipeline.create(opt);

    // TextIO writer
    TextIO.Write writer = getWriter(opt.getOutputFilePath());

    // define pipeline graph
    pipeline
        .apply("Create dummy data",
            Create.of("Sample text").withCoder(StringUtf8Coder.of()))
        .apply(writer);

    // execute
    pipeline.run();
  }
}
