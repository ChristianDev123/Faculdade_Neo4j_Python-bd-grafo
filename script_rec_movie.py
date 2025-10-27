from database import Database
import math

uri = 'neo4j://127.0.0.1:7687'
user = 'neo4j'
password = '1LZ_vndfSFgEWC9O8bg4zkr50kh9BV3IVNaL699eYTY'
database = Database(password,user,uri)

class SysRecMovie:
    def __init__(self, database:Database):
        self.database = database
    
    def get_all_movies(self, limit = None):
        return self.database.readDatabase(f"""
            MATCH (m:MOVIE)
            RETURN DISTINCT(m)
            ORDER BY m.id
            {'' if(limit is None) else f'LIMIT {limit}'}
        """)
    
    def get_all_users(self, limit = None):
        return self.database.readDatabase(f"""
            MATCH (u:USER)
            RETURN DISTINCT(u)
            ORDER BY u.id
            {'' if(limit is None) else f'LIMIT {limit}'}
        """)

    def get_all_users_reviewed_both_movies(self,m1Id,m2Id):
        return self.database.readDatabase(f"""
            MATCH (m)-[w]->(u)
            WHERE w.movieId in [{m1Id},{m2Id}]
            WITH u, collect(w.rating) as rating
            WHERE size(rating) = 2
            return u     
        """)
    
    def get_user_movie_rating(self, userId, movieId):
        return self.database.readDatabase(f"""
            MATCH ()-[w]->()
            WHERE w.movieId = {movieId}
            AND w.userId = {userId}
            return w.rating as rating
        """)

    def get_avg_rating_movie(self, movieId):
        return self.database.readDatabase(f"""
            MATCH ()-[w]->()
            WHERE w.movieId = {movieId}
            RETURN AVG(w.rating) as avg_rating
        """)

    def create_pearson_correl_m1_m2(self,m1Id,m2Id, correl):
        with self.database.drive.session() as session:
            session.execute_write(lambda tx: tx.run(f"""
                MATCH (m1:MOVIE {{id:{m1Id}}}), (m2:MOVIE {{id:{m2Id}}})
                CREATE (m1)-[:PEARSON {{ movie1Id:{m1Id}, movie2Id:{m2Id}, correl:{correl} }}]->(m2)
            """))

    def calc_pearson_correl(self):
        movies = self.get_all_movies()
        for i, movie1 in enumerate(movies):
            m1Id = movie1['m']['id']
            m1AvgRating = self.get_avg_rating_movie(m1Id)[0]['avg_rating']
            for j, movie2 in enumerate(movies):
                if(j<=i): continue
                m2Id = movie2['m']['id']
                m2AvgRating = self.get_avg_rating_movie(m2Id)[0]['avg_rating']   
                users_reviewed = self.get_all_users_reviewed_both_movies(m1Id,m2Id)
                numerator = 0
                denominatorm1 = 0
                denominatorm2 = 0
                for user in users_reviewed:
                    userId = user['u']['id']
                    m1UserRating = self.get_user_movie_rating(userId, m1Id)[0]['rating']
                    m2UserRating = self.get_user_movie_rating(userId, m2Id)[0]['rating']
                    numerator += (m1UserRating-m1AvgRating)*(m2UserRating-m2AvgRating)
                    denominatorm1 += (m1UserRating-m1AvgRating)**2
                    denominatorm2 += (m2UserRating-m2AvgRating)**2
                denominatorm1 = math.sqrt(denominatorm1)
                denominatorm2 = math.sqrt(denominatorm2)
                try:
                    pearson = numerator/(denominatorm1*denominatorm2)
                except:
                    person = 0
                self.create_pearson_correl_m1_m2(m1Id,m2Id,pearson)

    def get_correls_movie(self, movieId):
        return self.database.readDatabase(f"""
            MATCH (m1:MOVIE)-[c:PEARSON]-(m2:MOVIE)
            WHERE m1.id = {movieId}
            RETURN properties(c) as c
        """)

    def preview_rating_user_movie(self, userId, movieId):
        correls_movie = self.get_correls_movie(movieId)
        numerator = 0
        denominator = 0

        for correlObj in correls_movie:
            m2 = correlObj['c']['movie2Id']
            rating_movie = self.get_user_movie_rating(userId, m2)
            if(len(rating_movie) == 0): continue 
            rating_movie = rating_movie[0]['rating']
            pearson = correlObj['c']['correl']
            if(pearson <= 0): continue
            numerator += pearson * rating_movie
            denominator += abs(pearson)

        return numerator/denominator if(denominator != 0) else 0
    
    def save_rec_users(self):
        users = self.get_all_users(15)
        movies = self.get_all_movies(50)

        for user in users:
            for movie in movies:
                preview_rating = self.preview_rating_user_movie(user['u']['id'], movie['m']['id'])
                with self.database.drive.session() as session:
                    session.execute_write(lambda tx: tx.run(f"""
                        MATCH (m:MOVIE {{ id:{movie['m']['id']} }})
                        MATCH (u:USER {{ id:{user['u']['id']} }})
                        CREATE (u)-[:PREVIEW {{ userId:{user['u']['id']}, movieId:{movie['m']['id']}, rating:{preview_rating} }}]->(m)
                    """))
    

sysRecMovie = SysRecMovie(database)
sysRecMovie.save_rec_users()