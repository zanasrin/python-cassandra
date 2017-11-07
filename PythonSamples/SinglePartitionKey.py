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

session.execute('CREATE KEYSPACE IF NOT EXISTS cycling WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'3\' }');
session.execute('CREATE TABLE IF NOT EXISTS cycling.cyclist_name (id int, lastname text, firstname text, PRIMARY KEY (id))');

insert_data = session.prepare("INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (?,?,?)")
batch = BatchStatement()
batch.add(insert_data, (1, 'PRADES', 'Benjamin'))
batch.add(insert_data, (2, 'PHELAN', 'Adam'))
batch.add(insert_data, (3, 'LEBAS', 'Thomas'))
batch.add(insert_data, (4, 'ZAKARIN', 'Ilnur'))
batch.add(insert_data, (5, 'BETANCUR', 'Carlos'))
batch.add(insert_data, (6, 'GILBERT', 'Phillippe'))
batch.add(insert_data, (7, 'MARTIN', 'Daniel'))
batch.add(insert_data, (8, 'CHAVES', 'Johan Esteban'))

session.execute(batch)

print "\nSelecting ALL"
rows = session.execute('SELECT * FROM cycling.cyclist_name')
PrintTable(rows)

print "\nSelect Using One Partition key"
rows = session.execute('SELECT * FROM cycling.cyclist_name where id = 1')
PrintTable(rows)

print "\nAllow filtering"
rows = session.execute('SELECT * FROM cycling.cyclist_name where firstname = \'Daniel\' ALLOW FILTERING')
PrintTable(rows)

print "\nUpdate One Row"
session.execute('Update cycling.cyclist_name set lastname = \'name_changed\' where id = 1')

print "\nSelect updated Row"
rows = session.execute('Select * from cycling.cyclist_name where id = 1')
PrintTable(rows)

cluster.shutdown()
