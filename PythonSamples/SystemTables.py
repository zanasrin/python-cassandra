from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable

def PrintTable(rows):
    t = PrettyTable(['ID', 'FirstName', 'LastName'])
    for user_row in rows:
        t.add_row([user_row.id, user_row.firstname, user_row.lastname])
    print t

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider)
session = cluster.connect()

print "\nSchema keyspaces by query"
rows = session.execute('SELECT * FROM system_schema.keyspaces')
t = PrettyTable([ 'KeySpaceName'])
for user_row in rows:
    t.add_row([user_row.keyspace_name])
print t

print "\nSchema tables by query"
rows = session.execute('SELECT table_name FROM system_schema.tables')
t = PrettyTable([ 'TableName'])
for user_row in rows:
    t.add_row([user_row.table_name])
print t

cluster.shutdown()