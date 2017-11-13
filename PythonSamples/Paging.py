from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import datetime
import threading
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH

def PrintTable(rows):
    t = PrettyTable(['City', 'Month', 'Year', 'Usage'])
    for user_row in rows:
        t.add_row([user_row.city, user_row.month, user_row.year, user_row.usage])
    print t

class PagedResultHandler(object):

    def __init__(self, future):
        self.error = None
        self.finished_event = threading.Event()
        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)
    def handle_page(self, rows):
        PrintTable(rows)
        if self.future.has_more_pages:
            print "NEW PAGE." 
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()
    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()
    

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

session.execute('CREATE KEYSPACE IF NOT EXISTS Electricity  WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'3\' }');
session.execute('CREATE TABLE IF NOT EXISTS Electricity.Consumption (city text, month text, year int, usage int, PRIMARY KEY(city, month, year))');

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
insert_data = session.prepare("INSERT INTO Electricity.Consumption (city, month, usage, year) VALUES (?,?,?,?)")

batch = BatchStatement()
for i in range(0, 100):
    index =  datetime.date.today().month
    batch.add(insert_data, ('London', months[(index+i)%12], 1000+(10*(i+1)), 2016-(i/12)))
session.execute(batch)       

batch = BatchStatement()
for i in range(0, 100):
    index =  datetime.date.today().month
    batch.add(insert_data, ('New York', months[(index+i)%12], 2000+(10*(i+1)), 2016-(i/12)))
session.execute(batch)  

batch = BatchStatement()
for i in range(0, 100):
    index =  datetime.date.today().month
    batch.add(insert_data, ('Amsterdam', months[(index+i)%12],  3000+(10*(i+1)), 2016-(i/12)))
session.execute(batch)  

batch = BatchStatement()
for i in range(0, 100):
    index =  datetime.date.today().month
    batch.add(insert_data, ('Tokyo', months[(index+i)%12], 4000+(10*(i+1)), 2016-(i/12)))
session.execute(batch)  

batch = BatchStatement()
for i in range(0, 100):
    index =  datetime.date.today().month
    batch.add(insert_data, ('Paris', months[(index+i)%12],  3000+(10*(i+1)), 2016-(i/12)))
session.execute(batch)


print "\nSelecting With Paging. Each page will fetch 100 rows"
query = "SELECT * FROM Electricity.Consumption"
statement = SimpleStatement(query, fetch_size = 100)
future = session.execute_async(statement)
handler = PagedResultHandler(future)
handler.finished_event.wait()
if handler.error:
    raise handler.error
cluster.shutdown()
