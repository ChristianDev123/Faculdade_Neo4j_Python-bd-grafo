from database import Database
from script_rec_movie import SysRecMovie

uri = 'neo4j://127.0.0.1:7687'
user = 'neo4j'
password = '1LZ_vndfSFgEWC9O8bg4zkr50kh9BV3IVNaL699eYTY'
database = Database(password,user,uri)
sysRecMovie = SysRecMovie(database)

class Test:
    def __init__(self, system:SysRecMovie):
        self.system = system
    
    def main(self):
        return
test = Test