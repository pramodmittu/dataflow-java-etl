package com.example.etl;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;

public class DataflowIngest {

    public interface Options extends org.apache.beam.sdk.options.PipelineOptions {
        @Description("Path of the file(s) to read from, e.g. gs://bucket/input/*.txt")
        @Validation.Required
        String getInputFile();
        void setInputFile(String value);

        @Description("BigQuery table to write to: project:dataset.table")
        @Validation.Required
        String getOutputTable();
        void setOutputTable(String value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        TableSchema schema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("id").setType("STRING"),
                new TableFieldSchema().setName("name").setType("STRING"),
                new TableFieldSchema().setName("value").setType("FLOAT"),
                new TableFieldSchema().setName("event_ts").setType("STRING")
        ));

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply("ToTableRow", ParDo.of(new DoFn<String, TableRow>() {
                    @ProcessElement
                    public void processElement(@DoFn.Element String line, OutputReceiver<TableRow> out) {
                        try {
                            String[] parts = line.split("\\|");
                            if (parts.length < 4) return;
                            TableRow row = new TableRow();
                            row.set("id", parts[0]);
                            row.set("name", parts[1]);
                            try { row.set("value", Double.valueOf(parts[2])); } catch(Exception e){ row.set("value", null); }
                            row.set("event_ts", parts[3]);
                            out.output(row);
                        } catch (Exception ex) {
                            System.err.println("Failed to parse line: " + line);
                        }
                    }
                }))
                .apply("WriteToBQ", BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                );

        p.run().waitUntilFinish();
    }
}
