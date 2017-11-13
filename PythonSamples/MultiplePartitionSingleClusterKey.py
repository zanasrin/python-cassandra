from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH

def PrintTable(rows):
    t = PrettyTable(['Race Year', 'Race Name', 'Cyclist Name', 'Rank'])
    for user_row in rows:
        t.add_row([user_row.race_year, user_row.race_name, user_row.cyclist_name, user_row.rank])
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

session.execute('CREATE KEYSPACE IF NOT EXISTS cycling WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'3\' }');
session.execute('CREATE TABLE IF NOT EXISTS cycling.rank_by_year_and_name (race_year int, race_name text, cyclist_name text, rank int, PRIMARY KEY ((race_year, race_name), rank))');

insert_data = session.prepare("INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (?,?,?,?)")
batch = BatchStatement()
batch.add(insert_data, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Benjamin PRADES', 1))
batch.add(insert_data, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Adam PHELAN', 2))
batch.add(insert_data, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Thomas LEBAS', 3))
batch.add(insert_data, (2015, 'Giro d\'\'Italia - Stage 11 - Forli > Imola', 'Ilnur ZAKARIN', 1))
batch.add(insert_data, (2015, 'Giro d\'\'Italia - Stage 11 - Forli > Imola', 'Carlos BETANCUR', 2))
batch.add(insert_data, (2015, '4th Tour of Beijing', 'Phillippe GILBERT', 1))
batch.add(insert_data, (2015, '4th Tour of Beijing', 'Daniel MARTIN', 2))
batch.add(insert_data, (2015, '4th Tour of Beijing', 'Johan Esteban CHAVES', 3))
batch.add(insert_data, (2015, 'Giro d\'\'Italia - Stage 11 - Forli > Imola', 'Daniel MARTIN', 2))

session.execute(batch)

print "\nSelecting ALL"
rows = session.execute('SELECT * FROM cycling.rank_by_year_and_name')
PrintTable(rows)

print "\nSelect Using One Partition key"
rows = session.execute('SELECT * FROM cycling.rank_by_year_and_name where race_year = 2014 ALLOW FILTERING')
PrintTable(rows)

print "\nSelect Using Both Partition keys"
rows = session.execute('SELECT * FROM cycling.rank_by_year_and_name where race_year = 2015 and race_name = \'Tour of Japan - Stage 4 - Minami > Shinshu\'')
PrintTable(rows)

print "\nAllow Filtering"
rows = session.execute('SELECT * FROM cycling.rank_by_year_and_name where cyclist_name = \'Daniel MARTIN\' ALLOW FILTERING')
PrintTable(rows)

print "\nUpdate One Row"
session.execute('Update cycling.rank_by_year_and_name set cyclist_name = \'changed_Name\'  where race_year = 2015 and race_name = \'Tour of Japan - Stage 4 - Minami > Shinshu\' and rank = 1')

print "\nOrder by Descending"
rows = session.execute('SELECT * FROM cycling.rank_by_year_and_name order by rank DESC')
PrintTable(rows)

cluster.shutdown()
