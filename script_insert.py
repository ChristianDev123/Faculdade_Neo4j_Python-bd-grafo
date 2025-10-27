import pandas as pd
from database import Database

uri = 'neo4j://127.0.0.1:7687'
user = 'neo4j'
password = '1LZ_vndfSFgEWC9O8bg4zkr50kh9BV3IVNaL699eYTY'

movies = pd.read_csv("movies.csv", sep=';')
rating = pd.read_csv("ratings.csv", sep=';')

def createMovie(dictMovie):
    id = dictMovie['movieId']
    title = dictMovie['title'].replace("'", "")
    varname = f"film{id}"
    dictMovie['genres'] = str(dictMovie['genres']).strip()
    if (dictMovie['genres'] in ['(no genres listed)','nan']):
        genres = f"'{dictMovie['genres']}'" 
    else:
        arrGenres = [f"'{x}'" for x in dictMovie['genres'].split('|')]
        genres = f"[{','.join(arrGenres)}]"

    return f"CREATE ({varname}:MOVIE {{ id:{id}, title:'{title}', genres:{genres}}})"

def createUsers(dictRating):
    userId = int(dictRating['userId'])
    username = f'user{userId}'
    return f"CREATE ({username}:USER {{ id: {userId}, name: '{username}'}})"

def createRating(dictRating):
    userId = dictRating['userId']
    movieId = dictRating['movieId']
    rating = dictRating['rating']
    timestamp = dictRating['timestamp']
    moviename = f'film{movieId}'
    username = f'user{userId}'
    
    return f"CREATE ({moviename})-[:WATCHED {{ userId: {userId}, movieId: {movieId}, rating: {rating}, timestamp: {timestamp}}}]->({username})"

database = Database(password, user, uri)

command = list(map(lambda x: createMovie(x), movies.to_dict(orient='records')))
command += list(set(map(lambda x: createUsers(x), rating.to_dict(orient='records'))))
command += list(map(lambda x: createRating(x), rating.to_dict(orient='records')))

database.createDatabase('\n'.join(command))

# [EXEMPLO] Seleção de Filmes assistidos por um usuário em específico que assistiu filmes de comédia com nota maior ou igual à 4

# MATCH r=(m:MOVIE)-[w:WATCHED]->(u:USER)
# WHERE "Comedy" in m.genres
# AND w.userId = 1
# AND w.rating >= 4
# return r