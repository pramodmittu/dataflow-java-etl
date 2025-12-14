Prerequisites
------------------
java -version
mvn -version
gcloud version
-----------------

Create your GCP account. 
1) Set up our GCS bucket
2) Create a Bigquery dataset and one table. 

gsutil mb -p axp-lumi-444505 -l us-west1 gs://axp-dataflow-bucket

gsutil mkdir gs://axp-dataflow-bucket/dataflow-staging
gsutil mkdir gs://axp-dataflow-bucket/tmp
gsutil mkdir gs://axp-dataflow-bucket/input

-----------------------------------

GCS permissions

gsutil iam ch \
 serviceAccount:141533068647-compute@developer.gserviceaccount.com:roles/storage.objectAdmin \
 gs://axp-dataflow-bucket


 gcloud projects add-iam-policy-binding axp-lumi-444505 \
 --member="serviceAccount:141533068647-compute@developer.gserviceaccount.com" \
 --role="roles/dataflow.worker"

 gcloud projects add-iam-policy-binding axp-lumi-444505 \
 --member="serviceAccount:141533068647-compute@developer.gserviceaccount.com" \
 --role="roles/bigquery.dataEditor"

 KMS key add

 gcloud kms keys add-iam-policy-binding salary_encryption_key \
 --location us-west1 \
 --keyring salary_encryption \
 --project axp-lumi-444505 \
 --member serviceAccount:141533068647-compute@developer.gserviceaccount.com \
 --role roles/cloudkms.cryptoKeyEncrypterDecrypter

 ------------------------------

 dataflow-java-etl/
├── src/main/java/com/example/etl/
│   ├── DataflowIngest.java
│   ├── EncryptDoFn.java
│   └── PipelineOptions.java
├── src/test/java/
├── pom.xml
├── .gitignore
└── README.md

-----------------------
mvn clean compile
------------------------

Run locally

mvn exec:java \
 -Dexec.mainClass=com.example.etl.DataflowIngest \
 -Dexec.args="--runner=DirectRunner"

 --------------------

 mvn compile exec:java \
 -Dexec.mainClass=com.example.etl.DataflowIngest \
 -Dexec.args="--runner=DataflowRunner \
   --project=axp-lumi-444505 \
   --region=us-west1 \
   --stagingLocation=gs://axp-dataflow-bucket/dataflow-staging \
   --tempLocation=gs://axp-dataflow-bucket/tmp \
   --inputFile=gs://axp-dataflow-bucket/input/data.txt \
   --outputTable=axp-lumi-444505:etl_dataset.stage_table \
   --kmsKey=projects/axp-lumi-444505/locations/us-west1/keyRings/salary_encryption/cryptoKeys/salary_encryption_key" ( its a sample key, create your own)
-------------------------

**After completing these steps:**
Dataflow job runs successfully
Sensitive fields are encrypted
Data is reliably loaded into BigQuery
Pipeline is production-ready and reproducible

-----------Sample data-------------
6|Fiona|88000|2025-01-10T12:35:00Z
7|George|99000|2025-01-10T12:36:00Z
8|Hannah|72000|2025-01-10T12:37:00Z
9|Ian|115000|2025-01-10T12:38:00Z
10|Julia|83000|2025-01-10T12:39:00Z
11|Kevin|91000|2025-01-10T12:40:00Z
12|Laura|76000|2025-01-10T12:41:00Z
13|Michael|125000|2025-01-10T12:42:00Z
14|Nina|69000|2025-01-10T12:43:00Z
15|Oscar|87000|2025-01-10T12:44:00Z
16|Paula|94000|2025-01-10T12:45:00Z
17|Quentin|102000|2025-01-10T12:46:00Z
18|Rachel|81000|2025-01-10T12:47:00Z

----------------------------------

BQ data in table after successsful run 

11	1	Alice	CiQAFDM7I4judJmPaSw8SOePypeZlad1blpFfRBwQ7Atq7gJRdMSLgBM13CmK4J3dnmPHhX/qeYUbNoLxzwyRYIzbInGAlbmte+1q+uq4BhWLeRoMX0=	2025-01-10T12:30:00Z
12	2	Bob	CiQAFDM7I70by+BiYTzQVkW7e8WpgcvVmz63JeWo3OX0OYt+Hj8SLgBM13CmhRkXQLZNO5dN0wBR4qeNcZ1zpk9BvvV3as4fMgyzwyonjyD+coImxSw=	2025-01-10T12:31:00Z
13	3	Charlie	CiQAFDM7I8m0szTZlXJkMK80mYDkkJp/fatsTUTOdWKQgzzGmGYSLgBM13Cm+InIyZExmbltnDRzz74k9d3PnTIpRNBvcC4PA515r8ot6JOXm2MjxYg=	2025-01-10T12:32:00Z
14	4	Diana	CiQAFDM7IxET1ev1ZgwOVBRvFKiylY045q0xYsy16d1pCXLqaAQSLwBM13Cmry5LyXHQkXR7hrkd+Gilm0J3UPVPlktAHBBWjo8/9aK9QB2W5W0yyjKM	2025-01-10T12:33:00Z
15	5	Evan	CiQAFDM7I7HYC0xcvEsIH9wNUesf7p7VpvdFKWEXiC60rwc48ZcSLgBM13Cmt4E8S/dHqFxgycR5FQCJQ0FMoz7uPZBcGy1VIfE8VrHXBGSIVvHP7hs=	2025-01-10T12:34:00Z
16	6	Fiona	CiQAFDM7I/qNXkVlfk6XxxBIkPAe5PnTSUJaSzAkDUHUaul4HMcSLgBM13CmzIjjJrRVEn+oMSASi7sZ/sAy6Q/IPKYdjqvvuYwxd79SLEPTeWgbd4E=	2025-01-10T12:35:00Z
17	7	George	CiQAFDM7I0X2AcGTsLwmhK2QI66+B8HS9pAlsWz/XaVZUqqZ9fASLgBM13CmlgNAiN/5kHzJZZ+ZXYw11F10bcoHVS9fHh4rApbA19LjMTd7TMUXC3g=	2025-01-10T12:36:00Z
18	8	Hannah	CiQAFDM7I5CG5C3rBosYS1xCgKLEaBNMetx8ssRvzdm09+86hhUSLgBM13CmK/pAHM3/L1d3efleTJ5Jq7dMPhFkzthk0OVmpFRq2enJAbaWqAWar7M=	2025-01-10T12:37:00Z
19	9	Ian	CiQAFDM7I6tgBDlWK26CaoIilPJswgoCxzxzLW+kFcAZuwGtZa8SLwBM13Cmf+dtqi0H8pdYcGtMzt/octRgIu83XHeQQKAMTdnCDdljIZiJK0tMF9qA	2025-01-10T12:38:00Z
20	10	Julia	CiQAFDM7I3wC6jACD32vgIed+j+rRrtPFBi0j7HUjJ++6DL7GrkSLgBM13CmMfPcCfeKTYHdxCBkrc6iKaVN7Vzm9B7r55t3VcYXMUXUkgGUIn37o2Y=	2025-01-10T12:39:00Z
21	11	Kevin	CiQAFDM7I3c7YbvXId+pEfS17XWYjBqxLBVU43FEkwR47ZKLFbISLgBM13CmRek/Q2oM6ulpxZFD9V983MfT5Rr7hGBjIfuOC3/xbgpSvT3JU82PUNs=	2025-01-10T12:40:00Z
22	12	Laura	CiQAFDM7I0B45V3F7/H75i19uDRhtPo4DjVOSNlnTtz6LTOUjhMSLgBM13CmTIsN3ICcEaVW471irlPP3YTHKL0CJvZnNY6/PCH6jl2meXDwuubbX/c=	2025-01-10T12:41:00Z
23	13	Michael	CiQAFDM7I/K/yQF+vmWK7iDPm2tojfcAT1zvFGpjoFH1PjJzHLgSLwBM13Cmmmk9Hi6Kg/gzbIdMOaHUZ8f2EZi/5y6+zVq0ur0BkFmXpAXUZY3Zn/N+	2025-01-10T12:42:00Z
24	14	Nina	CiQAFDM7IyGIuysvD25gqZkZdfUh9pFtpHo59GegJeTzVb4D4EwSLgBM13CmPxleVYxdE8IFd3I9hYbuwq/0teJ4zuGFvCiJjA2pl2aEWoQJ+mc54Do=	2025-01-10T12:43:00Z
25	15	Oscar	CiQAFDM7I28mW525tKkqbmGdoP8hn08nnHrNUN7pzvQ0MlS3tPsSLgBM13CmcxO4Zn7z6yozfUyvXvTMVg0QP8EAjzftLztLbY2NS1hH6hQU7VJvfPU=	2025-01-10T12:44:00Z
26	16	Paula	CiQAFDM7Iwc4wKSywpEnMTy1swgAOiHILTCsqV9mH9PHs26SBWkSLgBM13CmGOHCqduj/5l/D3sArG88O5WOhzYzSwqwWquBf8+529X4gCfydL+vTKo=	2025-01-10T12:45:00Z
27	17	Quentin	CiQAFDM7I2WzsqJZWLarP2NLKcQ5askaJo7IhHYKscofiGyxBLMSLgBM13CmgbSUx+5zrDHrQeOcZ0CPh1Y2PC8drV3PyN3Mlk3QjttvHFdssrUYSo8=	2025-01-10T12:46:00Z
28	18	Rachel	CiQAFDM7I3X8f8zh+Vts2bn57jzoyZWMzWkavlSSznjc1FcX0TwSLgBM13Cm0qJUAQp+QmrbPneug07Ze1AHWhQFej8yYYERrKO/XbAG/9clBLyqFGY=	2025-01-10T12:47:00Z
29	19	Steve	CiQAFDM7IwCm6/+fpOvDHzzn1gLCRzpKqOqUBGmq1kNWL5TqwYcSLQBM13Cm7vFX+m9BS1vCeX1aOhj+qlsYSiHhcZhSG/zZr1N52ha9eKoIzPYwBw==	2025-01-10T12:48:00Z
30	20	Tina	CiQAFDM7I4FJt+R+5ED8OHOHvz/YJAsJ+bzZTuZBsLeFBUoxc5gSLgBM13CmBWu06RbNUWwuEhh7xNz/vFGORiP3r94PXI1PschU11nScIYgMk5O2pk=	2025-01-10T12:49:00Z

Enjoy!!
 
