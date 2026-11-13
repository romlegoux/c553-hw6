import os
import sys
import pika
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def main():

    if len(sys.argv) < 2:
        print("Usage: ./setup.py <QNAME>")
        sys.exit(1)
        
    qname = sys.argv[1]
    print(f"Starting setup for run: {qname}")

    #1 RabbitMQ
    print("\n Setting up RabbitMQ queues")
    try:
        url = os.environ["CK_AMQP_URL"]
        params = pika.URLParameters(url)
        conn = pika.BlockingConnection(params)
        channel = conn.channel()

        for q_suffix in ["tasks", "results"]:
            q_name = f"{qname}.{q_suffix}"
            channel.queue_declare(
                queue=q_name, 
                durable=True, 
                arguments={"x-queue-type": "quorum"}
            )
            print(f"RabbitMQ OK : Queue '{q_name}' declared.")
        
        conn.close()
    except Exception as e:
        print(f"RabbitMQ FAILED: {e}")
        sys.exit(1)

    #2 Cassandra Setup
    print("\n Setting up Cassandra")
    try:
        hosts = os.environ["CK_CASSANDRA_HOSTS"].split(",")
        user = os.environ["CK_CASSANDRA_USER"]
        password = os.environ["CK_CASSANDRA_PASSWORD"]

        auth = PlainTextAuthProvider(username=user, password=password)
        cluster = Cluster(contact_points=hosts, auth_provider=auth)
        session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS ck 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        session.set_keyspace('ck')

        session.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                run text,
                id text,
                command text,
                state text,
                submit_time timestamp,
                PRIMARY KEY ((run, id))
            );
        """)
        
        session.execute("""
            CREATE TABLE IF NOT EXISTS claims (
                run text,
                id text,
                worker text,
                PRIMARY KEY ((run, id))
            );
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS counters (
                run text,
                metric text,
                count counter,
                PRIMARY KEY (run, metric)
            );
        """)

        print("Cassandra OK ")
        cluster.shutdown()

    except Exception as e:
        print(f"Cassandra FAILED : {e}")
        sys.exit(1)

    print("\nSetup finished ok")

if __name__ == "__main__":
    main()
