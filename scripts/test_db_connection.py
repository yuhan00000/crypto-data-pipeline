import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="crypto_db",
    user="postgres",
    password="admin7876",
    port="5432"
)

print("Connected successfully!")

conn.close()