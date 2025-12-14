package com.example.etl; // <--- ADD THIS LINE

import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import java.util.Base64;
// Note: This local test uses System.out.println instead of SLF4J/LOG

public class TestEncryption {

    public static void main(String[] args) throws Exception {
        // --- Configuration ---
        // Ensure this key path is correct and your local gcloud application default credentials
        // have the 'Cloud KMS CryptoKey Encrypter/Decrypter' role.
        String kmsKeyPath = "projects/axp-lumi-444505/locations/us-west1/keyRings/salary_encryption/cryptoKeys/salary_encryption_key";

        System.out.println("Starting local encryption test with key: " + kmsKeyPath);

        EncryptSalaryFn fn = new EncryptSalaryFn(kmsKeyPath);

        String salary = "10000";
        String encrypted = fn.encrypt(salary);

        System.out.println("\nOriginal salary: " + salary);

        if (encrypted != null) {
            System.out.println("Encrypted salary (Base64): " + encrypted);
        } else {
            System.out.println("Encryption failed! Result is null.");
        }
    }

    // This nested class mimics the essential logic of your Dataflow DoFn
    public static class EncryptSalaryFn {

        private final String kmsKeyPath;
        // Keep it transient, demonstrating it's non-serializable across workers (processes)
        private transient KeyManagementServiceClient kmsClient;

        public EncryptSalaryFn(String kmsKeyPath) {
            this.kmsKeyPath = kmsKeyPath;
        }

        // Lazy initialization to ensure the client is created upon first use
        private KeyManagementServiceClient getKmsClient() throws Exception {
            if (kmsClient == null) {
                System.out.println("Lazily initializing KMS client...");
                kmsClient = KeyManagementServiceClient.create();
            }
            return kmsClient;
        }

        // Make encrypt() public for testing
        public String encrypt(String plaintext) {
            if (plaintext == null || plaintext.trim().isEmpty()) {
                System.out.println("Plaintext is empty, skipping encryption.");
                return null;
            }

            try {
                System.out.println("Encrypting plaintext: " + plaintext);

                // FIX: Use the lazy-initialized client via getKmsClient()
                EncryptResponse resp = getKmsClient().encrypt(
                        CryptoKeyName.parse(kmsKeyPath),
                        ByteString.copyFromUtf8(plaintext)
                );

                byte[] ciphertext = resp.getCiphertext().toByteArray();
                if (ciphertext == null || ciphertext.length == 0) {
                    System.out.println("Encryption response is empty!");
                    return null;
                }

                // Returns the Base64 encoded ciphertext
                return Base64.getEncoder().encodeToString(ciphertext);

            } catch (Exception e) {
                System.out.println("KMS encryption ERROR: " + e.getMessage());
                // In case of any exception (like permission or network issues), return null
                return null;
            }
        }

        // Add a placeholder for cleanup (though not strictly necessary for a simple local main method)
        // @Teardown would be here in the Beam DoFn
        public void cleanup() {
            if (kmsClient != null) {
                kmsClient.close();
                System.out.println("KMS client closed.");
            }
        }
    }
}