import argparse
import random
from time import sleep

import boto3
import psycopg2
from botocore.client import Config
from botocore.exceptions import ClientError
from faker import Faker


def create_s3_client(access_key, secret_key, endpoint, region):
    """
    Create a boto3 client configured for Minio or any S3-compatible service.

    :param access_key: S3 access key
    :param secret_key: S3 secret key
    :param endpoint: Endpoint URL for the S3 service
    :param region: Region to use, defaults to us-east-1
    :return: Configured S3 client
    """
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )


def create_bucket_if_not_exists(s3_client, bucket_name):
    """
    Check if an S3 bucket exists, and if not, create it.

    :param s3_client: Configured S3 client
    :param bucket_name: Name of the bucket to create or check
    :return: None
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            # Bucket does not exist, create it
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' created.")
            except ClientError as error:
                print(f"Failed to create bucket: {error}")
        else:
            print(f"Error: {e}")


fake = Faker()


def gen_user_product_data(num_records: int) -> None:
    for id in range(num_records):
        sleep(0.5)
        conn = psycopg2.connect(
            "dbname='postgres' user='postgres' host='postgres' password='postgres'"
        )
        curr = conn.cursor()
        curr.execute(
            "INSERT INTO commerce.users (id, username, password) VALUES (%s, %s, %s)",
            (id, fake.user_name(), fake.password()),
        )
        curr.execute(
            "INSERT INTO commerce.products (id, name, description, price) VALUES (%s, %s, %s, %s)",
            (id, fake.name(), fake.text(), fake.random_int(min=1, max=100)),
        )
        conn.commit()

        sleep(0.5)
        # update 10 % of the time
        if random.randint(1, 100) >= 90:
            curr.execute(
                "UPDATE commerce.users SET username = %s WHERE id = %s",
                (fake.user_name(), id),
            )
            curr.execute(
                "UPDATE commerce.products SET name = %s WHERE id = %s",
                (fake.name(), id),
            )
        conn.commit()

        sleep(0.5)
        # delete 5 % of the time
        if random.randint(1, 100) >= 95:
            curr.execute("DELETE FROM commerce.users WHERE id = %s", (id,))
            curr.execute("DELETE FROM commerce.products WHERE id = %s", (id,))

        conn.commit()
        curr.close()

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--num_records",
        type=int,
        help="Number of records to generate",
        default=1000,
    )
    args = parser.parse_args()

    # Credentials and Connection Info
    access_key = "minio"
    secret_key = "minio123"
    endpoint = "http://minio:9000"
    region = "us-east-1"

    s3_client = create_s3_client(access_key, secret_key, endpoint, region)
    bucket_name = "commerce"  # Replace with your bucket name
    create_bucket_if_not_exists(s3_client, bucket_name)
    num_records = args.num_records

    gen_user_product_data(num_records)
