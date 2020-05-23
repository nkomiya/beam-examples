package com.examples.beam.reader;

import com.examples.beam.commons.PCollectionDumpFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ParDo;


/**
 * Sample code for TextIO reader
 */
public class TextIOReader {
  /**
   * Interface for pipeline options
   */
  public interface CustomOptions extends PipelineOptions {
    @Validation.Required
    String getFilePattern();

    void setFilePattern(String s);
  }

  /**
   * Get TextIO reader
   *
   * @param filePattern Glob file pattern for input files
   * @return TextIO reader
   */
  static TextIO.Read getReader(String filePattern) {
    return TextIO
        .read()
        .from(filePattern);
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

    // TextIO reader
    TextIO.Read reader = TextIOReader.getReader(opt.getFilePattern());

    // define pipeline graph
    pipeline
        .apply("Read text", reader)
        .apply("Output elements", ParDo.of(new PCollectionDumpFn<>()));

    // run pipeline
    pipeline.run();
  }
}
