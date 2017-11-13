from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH

def PrintTable(rows):
    t = PrettyTable(['ID', 'FirstName', 'LastName'])
    for user_row in rows:
        t.add_row([user_row.id, user_row.firstname, user_row.lastname])
    print t
    
ssl_opts = {
    'ca_certs': DEFAULT_CA_BUNDLE_PATH,
    'ssl_version': PROTOCOL_TLSv1_2,
}
if 'selfsigned_cert' in cfg.config:
    ssl_opts['ca_certs'] = cfg.config['selfsigned_cert']
auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider, ssl_options=ssl_opts)
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
