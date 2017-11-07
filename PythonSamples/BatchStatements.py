from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider)
session = cluster.connect()

session.execute('CREATE KEYSPACE IF NOT EXISTS cycling WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'3\' }');
session.execute('CREATE TABLE IF NOT EXISTS cycling.rank_by_year_and_name (race_year int, race_name text, cyclist_name text, rank int, PRIMARY KEY ((race_year, race_name), rank))');


insert_user = session.prepare("INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES(?,?,?,?)")
batch = BatchStatement()

batch.add(insert_user, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Benjamin PRADES', 1))
batch.add(insert_user, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Adam PHELAN', 2))
batch.add(insert_user, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Thomas LEBAS', 3))
batch.add(insert_user, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Ilnur ZAKARIN', 1))
batch.add(insert_user, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Carlos BETANCUR', 4))
batch.add(insert_user, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Phillippe GILBERT', 5))
batch.add(insert_user, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Daniel MARTIN', 6))
batch.add(insert_user, (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Johan Esteban CHAVES', 7))
batch.add(insert_user, (2013, 'Giro d\'\'Italia - Stage 11 - Forli > Imola', 'Daniel MARTIN', 2))

batch.add(SimpleStatement("UPDATE cycling.rank_by_year_and_name set cyclist_name = \'name-changed\' where race_year = 2013 and race_name = \'Giro d\'\'Italia - Stage 11 - Forli > Imola\' and rank = 2"))
batch.add(SimpleStatement("UPDATE cycling.rank_by_year_and_name set cyclist_name = \'name-changed1\' where race_year = 2014 and race_name = \'4th Tour of Beijing\' and rank = 2"))
session.execute(batch)

rows = session.execute('SELECT * FROM cycling.rank_by_year_and_name')
t = PrettyTable(['Race Year', 'Race Name', 'Cyclist Name', 'Rank'])
for user_row in rows:
    t.add_row([user_row.race_year, user_row.race_name, user_row.cyclist_name, user_row.rank])
print t

cluster.shutdown()
