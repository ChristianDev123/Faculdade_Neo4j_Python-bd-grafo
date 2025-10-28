from database import Database
from script_rec_movie import SysRecMovie
import math

uri = 'neo4j://127.0.0.1:7687'
user = 'neo4j'
password = '1LZ_vndfSFgEWC9O8bg4zkr50kh9BV3IVNaL699eYTY'
database = Database(password,user,uri)
sysRecMovie = SysRecMovie(database)

class Test:
    def __init__(self, system:SysRecMovie):
        self.system = system
    
    def get_all_rating_preview(self):
        return self.system.database.readDatabase("""
            MATCH ()-[p:PREVIEW]-()
            return properties(p) as p
        """)

    def get_n_top_preview_rating(self, userId, n=5):
        return self.system.database.readDatabase(f"""
            MATCH ()-[p:PREVIEW]->()
            WHERE p.userId = {userId}
            RETURN properties(p)
            ORDER BY p.rating desc
            LIMIT {n}
        """)

    def get_watched_movies(self, userId, n=5):
        return self.system.database.readDatabase(f"""
            MATCH ()-[w:WATCHED]->()
            WHERE w.userId = {userId}
            RETURN w
            LIMIT {n}
        """)
    
    def main(self):
        precision, recall = [], []
        users = self.system.get_all_users(12) #80% do modelo
        for user in users:
            userId = user['u']['id']
            watched_movies = self.get_watched_movies(userId)
            recommended = self.get_n_top_preview_rating(userId)
            precision.append(len(watched_movies+recommended)/len(recommended))
            recall.append(len(watched_movies+recommended)/len(watched_movies))
        
        print(f"precision: {sum(precision)/len(precision)}, recall: {sum(recall)/len(recall)}")

test = Test(sysRecMovie)
test.main()