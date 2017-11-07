from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider)
session = cluster.connect('electricity')
rows = session.execute('SELECT * FROM consumption')
for user_row in rows:
    print user_row.city, user_row.month, user_row.year


