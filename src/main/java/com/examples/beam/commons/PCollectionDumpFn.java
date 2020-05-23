package com.examples.beam.commons;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DoFn sub class to output {@code PCollection} elements
 *
 * @param <InputT> Type of {@code PCollection}
 */
public class PCollectionDumpFn<InputT> extends DoFn<InputT, Void> {
  /** logger */
  private final Logger LOGGER = LoggerFactory.getLogger(PCollectionDumpFn.class);

  @ProcessElement
  public void processElement(@Element InputT input) {
    this.LOGGER.info(input.toString());
  }
}
