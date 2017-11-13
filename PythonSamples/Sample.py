from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH

ssl_opts = {
    'ca_certs': DEFAULT_CA_BUNDLE_PATH,
    'ssl_version': PROTOCOL_TLSv1_2,
}
if 'selfsigned_cert' in cfg.config:
    ssl_opts['ca_certs'] = cfg.config['selfsigned_cert']

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider, ssl_options=ssl_opts)
session = cluster.connect('electricity')

rows = session.execute('SELECT * FROM consumption')
for user_row in rows:
    print user_row.city, user_row.month, user_row.year


