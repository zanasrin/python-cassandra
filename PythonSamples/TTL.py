from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import time

def PrintTable(rows):
    t = PrettyTable(['ID', 'RaceName', 'StartDate', 'EndDate'])
    for user_row in rows:
        t.add_row([user_row.race_id, user_row.race_name, user_row.race_start_date, user_row.race_end_date])
    print t

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider)
session = cluster.connect()

session.execute('CREATE KEYSPACE IF NOT EXISTS Cycling WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'3\' }');
session.execute('CREATE TABLE IF NOT EXISTS Cycling.calendar (race_id int PRIMARY KEY, race_name text, race_start_date text, race_end_date text) WITH default_time_to_live = 120');

insert_data = session.prepare("INSERT INTO Cycling.calendar (race_id, race_name, race_start_date, race_end_date) VALUES (?,?,?,?)")
batch = BatchStatement()
batch.add(insert_data, (1, 'Tour de France - Stage 12', '2017-10-30', '2017-10-31'))
batch.add(insert_data, (2, 'Tour de France - Stage 12', '2017-09-26', '2017-09-27'))
batch.add(insert_data, (3, 'Tour de France - Stage 13', '2017-08-01', '2017-08-02'))
batch.add(insert_data, (4, 'Tour de France - Stage 14', '2017-10-30', '2017-10-31'))
batch.add(insert_data, (5, '4th Tour of Beijing', '2017-09-15', '2017-09-16'))
batch.add(insert_data, (6, '4th Tour of Beijing', '2017-10-30', '2017-10-31'))

session.execute(batch)

print "\nSelecting ALL"
rows = session.execute('SELECT * FROM Cycling.calendar')
PrintTable(rows)

print "\n Sleeping for 3 minutes"
time.sleep(180)

print "\nSelecting ALL"
rows = session.execute('SELECT * FROM Cycling.calendar')
PrintTable(rows)

cluster.shutdown()