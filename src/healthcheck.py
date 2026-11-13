#!/usr/bin/env python3
import os
import sys
import pika
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import boto3
from botocore.config import Config

def main():
    print("Starting healthcheck")
    errors = 0

    # 1. Check RabbitMQ
    try:
        print("\n Checking RabbitMQ")
        url = os.environ["CK_AMQP_URL"]
        params = pika.URLParameters(url)
        conn = pika.BlockingConnection(params)
        server_props = conn._impl.server_properties
        version = server_props.get('version', b'unknown').decode()
        
        print(f"RabbitMQ OK {version}")
        conn.close()
    except Exception as e:
        print(f"RabbitMQ FAILED : {e}")
        errors += 1

    # 2. Check Cassandra
    try:
        print("\n Checking Cassandra")
        # Split the hosts string just in case there are multiple IPs
        hosts = os.environ["CK_CASSANDRA_HOSTS"].split(",")
        user = os.environ["CK_CASSANDRA_USER"]
        password = os.environ["CK_CASSANDRA_PASSWORD"]

        auth = PlainTextAuthProvider(username=user, password=password)
        cluster = Cluster(contact_points=hosts, auth_provider=auth)
        session = cluster.connect()
        
        # The version is stored in the system.local table
        row = session.execute("SELECT release_version FROM system.local").one()
        print(f"Cassandra OK {version}")
        cluster.shutdown()
    except Exception as e:
        print(f"Cassandra FAILED : {e}")
        errors += 1

    # 3. Check SeaweedFS
    try:
        print("\n[+] Checking SeaweedFS")

        s3 = boto3.client(
            's3',
            endpoint_url=os.environ["CK_S3_ENDPOINT"],
            aws_access_key_id=os.environ["CK_S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["CK_S3_SECRET_KEY"],
            config=Config(s3={'addressing_style': 'path'})
        )

        res = s3.list_buckets()
        
        headers = res.get('ResponseMetadata', {}).get('HTTPHeaders', {})
        server_header = headers.get('server', 'unknown')
        
        print(f"SeaweedFS OK")
    except Exception as e:
        print(f"SeaweedFS FAILED : {e}")
        errors += 1

    print("\n--- Healthcheck Summary ---")
    if errors == 0:
        print("The 3 services are up and running ")
        sys.exit(0)
    else:
        print(f"Failed : {errors} services are currently unreachable.")
        sys.exit(1)

if __name__ == "__main__":
    main()
