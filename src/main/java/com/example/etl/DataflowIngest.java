package com.example.etl;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Base64;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowIngest {

    /** Pipeline options interface */
    public interface Options extends org.apache.beam.sdk.options.PipelineOptions {

        @Description("Input file path")
        @Validation.Required
        String getInputFile();
        void setInputFile(String value);

        @Description("Output BigQuery table: project:dataset.table")
        @Validation.Required
        String getOutputTable();
        void setOutputTable(String value);

        @Description("KMS key path")
        @Validation.Required
        String getKmsKey();
        void setKmsKey(String value);
    }

    /** DoFn to encrypt the salary field */
    // DoFn to encrypt the salary field
    public static class EncryptSalaryFn extends DoFn<String, TableRow> {
        private static final Logger LOG = LoggerFactory.getLogger(EncryptSalaryFn.class);

        // KMS client must be transient as it is not serializable.
        private transient KeyManagementServiceClient kmsClient;
        private final String kmsKeyPath;

        public EncryptSalaryFn(String kmsKeyPath) {
            this.kmsKeyPath = kmsKeyPath;
        }

        // Helper method to lazily initialize the non-serializable KMS client.
        private KeyManagementServiceClient getKmsClient() throws Exception {
            if (kmsClient == null) {
                LOG.info("Lazily initializing KMS client on worker for key: {}", kmsKeyPath);
                kmsClient = KeyManagementServiceClient.create();
            }
            return kmsClient;
        }

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<TableRow> out) {

            LOG.info("Processing line: {}", line);

            String[] parts = line.split("\\|");
            if (parts.length < 4) {
                LOG.error("Invalid row format: {}", line);
                return;
            }

            String id = parts[0];
            String name = parts[1];
            String salary = parts[2];
            String eventTs = parts[3];

            LOG.info("Extracted salary '{}'", salary);

            if (salary == null || salary.trim().isEmpty()) {
                LOG.error("Salary field is EMPTY or NULL for id {}", id);
            }

            String encryptedSalary = encrypt(salary);

            if (encryptedSalary == null) {
                LOG.error("Encryption FAILED for salary '{}' (id={})", salary, id);
            }

            TableRow row = new TableRow()
                    .set("id", id)
                    .set("name", name)
                    .set("salary_enc", encryptedSalary)
                    .set("event_ts", eventTs);

            out.output(row);
        }

        private String encrypt(String plaintext) {
            if (plaintext == null || plaintext.trim().isEmpty()) {
                LOG.warn("Plaintext is empty, skipping encryption.");
                return null;
            }

            try {
                LOG.info("Encrypting plaintext salary: '{}' using key {}", plaintext, kmsKeyPath);

                // FIX: Call getKmsClient() to ensure the client is initialized.
                EncryptResponse resp = getKmsClient().encrypt(
                        CryptoKeyName.parse(kmsKeyPath),
                        ByteString.copyFromUtf8(plaintext)
                );

                byte[] ciphertext = resp.getCiphertext().toByteArray();
                if (ciphertext == null || ciphertext.length == 0) {
                    LOG.error("Encryption response is empty!");
                    return null;
                }

                String encoded = Base64.getEncoder().encodeToString(ciphertext);
                LOG.info("Encrypted salary successfully.");
                return encoded;

            } catch (Exception e) {
                LOG.error("KMS encryption ERROR for value '{}': {}", plaintext, e.getMessage(), e);
                return null;
            }
        }

        @Teardown
        public void teardown() {
            if (kmsClient != null) {
                LOG.info("Closing KMS client.");
                kmsClient.close();
                kmsClient = null;
            }
        }
    }

    /** Main method to run the Dataflow pipeline */
    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        TableSchema schema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("id").setType("STRING"),
                new TableFieldSchema().setName("name").setType("STRING"),
                new TableFieldSchema().setName("salary_enc").setType("STRING"),
                new TableFieldSchema().setName("event_ts").setType("STRING")
        ));

        p.apply("Read Input", TextIO.read().from(options.getInputFile()))
                .apply("Encrypt Salary", ParDo.of(new EncryptSalaryFn(options.getKmsKey())))
                .apply("WriteToBQ", BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withSchema(schema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                );

        p.run().waitUntilFinish();
    }
}
