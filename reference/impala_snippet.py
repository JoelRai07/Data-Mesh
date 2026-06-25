import os
from impala.dbapi import connect
from dotenv import load_dotenv

load_dotenv()

IMPALA_HOST = os.getenv("IMPALA_HOST")
IMPALA_PORT = int(os.getenv("IMPALA_PORT"))
IMPALA_HTTP_PATH = os.getenv("IMPALA_HTTP_PATH")
IMPALA_USER = os.getenv("IMPALA_USER")
IMPALA_PASSWORD = os.getenv("IMPALA_PASSWORD")

connection = connect(
    host = IMPALA_HOST,
    port = IMPALA_PORT,
    auth_mechanism = "LDAP",
    use_ssl = True,
    use_http_transport = True,
    http_path = IMPALA_HTTP_PATH,
    user = IMPALA_USER,
    password = IMPALA_PASSWORD)

cursor = connection.cursor()

#Beispiel: Alle Daten aus der Tabelle "actor" abrufen und ausgeben
cursor.execute("SELECT * from actor limit 10")
for row in cursor.fetchall():
    print(row)