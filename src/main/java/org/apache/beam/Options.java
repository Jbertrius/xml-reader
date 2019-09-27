package org.apache.beam;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends DataflowPipelineOptions {
  @Description("The GCS location of the text you'd like to process")
  ValueProvider<String> getInputFilePattern();

  void setInputFilePattern(ValueProvider<String> value);

  @Description("JSON file with BigQuery Schema description")
  ValueProvider<String> getJSONPath();

  void setJSONPath(ValueProvider<String> value);

  @Description("Output topic to write to")
  ValueProvider<String> getOutputTable();

  void setOutputTable(ValueProvider<String> value);

//  @Description("GCS path to javascript fn for transforming output")
//  ValueProvider<String> getJavascriptTextTransformGcsPath();
//
//  void setJavascriptTextTransformGcsPath(ValueProvider<String> jsTransformPath);

//  @Validation.Required
//  @Description("UDF Javascript Function Name")
//  ValueProvider<String> getJavascriptTextTransformFunctionName();
//
//  void setJavascriptTextTransformFunctionName(
//          ValueProvider<String> javascriptTextTransformFunctionName);

//  @Validation.Required
  @Description("Temporary directory for BigQuery loading process")
  ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

  void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
}
