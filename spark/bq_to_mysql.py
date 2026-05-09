import argparse
from pyspark.sql import SparkSession
from google.cloud import secretmanager

def get_password(secret_name, project):
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{project}/secrets/{secret_name}/versions/latest"
    resp   = client.access_secret_version(request={"name": name})
    return resp.payload.data.decode("UTF-8")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",               required=True)
    parser.add_argument("--bq_dataset",            required=True)
    parser.add_argument("--bq_table",              required=True)
    parser.add_argument("--mysql_host",            required=True)
    parser.add_argument("--mysql_db",              required=True)
    parser.add_argument("--mysql_table",           required=True)
    parser.add_argument("--mysql_user",            required=True)
    parser.add_argument("--mysql_password_secret", required=True)
    parser.add_argument("--temp_gcs_bucket",       required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("BQToMySQL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading: {args.bq_dataset}.{args.bq_table}")
    df = spark.read.format("bigquery") \
        .option("table", f"{args.project}.{args.bq_dataset}.{args.bq_table}") \
        .load()

    print(f"Rows: {df.count()}")

    password  = get_password(args.mysql_password_secret, args.project)
    mysql_url = f"jdbc:mysql://{args.mysql_host}:3306/{args.mysql_db}"

    print(f"Writing to MySQL: {mysql_url}")
    df.write.format("jdbc") \
        .option("url",      mysql_url) \
        .option("driver",   "com.mysql.cj.jdbc.Driver") \
        .option("dbtable",  args.mysql_table) \
        .option("user",     args.mysql_user) \
        .option("password", password) \
        .mode("overwrite") \
        .save()

    print("✅ Done!")
    spark.stop()

if __name__ == "__main__":
    main()
