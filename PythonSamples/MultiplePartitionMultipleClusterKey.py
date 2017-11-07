from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable

def PrintTable(rows):
    t = PrettyTable(['Votes', 'Song Name', 'Artist Name', 'Album Name', 'Year'])
    for user_row in rows:
        t.add_row([user_row.votes, user_row.song_name, user_row.artist_name, user_row.album_name, user_row.year])
    print t

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider)
session = cluster.connect()

session.execute('CREATE KEYSPACE IF NOT EXISTS Music WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'3\' }');
session.execute('CREATE TABLE IF NOT EXISTS Music.playlists (votes int, song_name text, artist_name text, album_name text, year text, PRIMARY KEY ((song_name, artist_name), album_name, year))');

insert_song = session.prepare("INSERT INTO Music.playlists (votes, song_name, artist_name, album_name, year) VALUES (?,?,?,?,?)")
batch = BatchStatement()
batch.add(insert_song, (None, 'Despacito', 'Luis Fonsi', 'undefined', '2017'))
batch.add(insert_song, (None, 'Shape of You', 'Ed Sheeran', 'undefined', '2017'))
batch.add(insert_song, (1000, 'If I could fly', 'One Direction', 'Made in the A.M', '2015'))
batch.add(insert_song, (2000, 'Happily', 'One Direction', 'Midnight Memories', '2013'))
batch.add(insert_song, (3000, 'One more night', 'Maroon 5', 'Overexposed', '2012'))
batch.add(insert_song, (4000, 'Sugar', 'Maroon 5', 'V', '2014'))

session.execute(batch)

print "\nSelecting ALL"
rows = session.execute('SELECT * FROM Music.playlists')
PrintTable(rows)

print "\nSelect Using One Partition key"
rows = session.execute('SELECT * FROM Music.playlists where artist_name = \'Maroon 5\' ALLOW FILTERING')
PrintTable(rows)

print "\nUpdate One Row"
session.execute('Update Music.playlists set votes = 10000 where song_name=\'Despacito\' and artist_name=\'Luis Fonsi\' and album_name = \'undefined\' and year = \'2017\'')

print "\nSelecting ALL"
rows = session.execute('SELECT * FROM Music.playlists')
PrintTable(rows)

print "\nSelect LIMIT 3"
rows = session.execute('SELECT * FROM Music.playlists LIMIT 3')
PrintTable(rows)

print "\nSelect with comparison operator"
rows = session.execute('SELECT * FROM Music.playlists where votes > 4000 ALLOW FILTERING')
PrintTable(rows)

print "\nDelete Row"
rows = session.execute('DELETE FROM Music.playlists where song_name=\'Despacito\' and artist_name=\'Luis Fonsi\' and album_name = \'undefined\' and year = \'2017\'')
PrintTable(rows)

print "\nSelecting ALL"
rows = session.execute('SELECT * FROM Music.playlists')
PrintTable(rows)

print "\nSchema keyspaces by query"
rows = session.execute('SELECT * FROM system_schema.keyspaces')
for row in rows:
    print row.keyspace_name

print "\nDropping table"
rows = session.execute('DROP Table Music.playlists')

print "\nDropping Keyspace"
rows = session.execute('DROP Keyspace Music')

cluster.shutdown()
