from bs4 import BeautifulSoup
import json
import requests
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

MOVIEDB_API_KEY = "API_KEY_HERE"

MoviesDF = pd.DataFrame(columns=['Title','Input Title','IMDB URL','IMDB Rating','Duration','Genres'])
TV_ShowsDF = pd.DataFrame(columns=['Title','Input Title','IMDB URL','IMDB Rating','Genres'])
Non_IMDB_URLsDF = pd.DataFrame(columns=['URL'])
ErrorsDF = pd.DataFrame(columns=['Input Name','Input URL'])
#PeopleDF = pd.DataFrame()

with open("Movies.html") as fp:
    soup = BeautifulSoup(fp, "html.parser")

def get_details(id,type):
        if type == "Movie":
            query = "https://api.themoviedb.org/3/movie/" + str(id)
        elif type == "TV":
            query = "https://api.themoviedb.org/3/tv/" + str(id)
        params = {
            "api_key" : MOVIEDB_API_KEY,
            "language" : "en-US"
        }
        response = requests.get(query,params=params)
        if response.status_code == 200:
            x = json.loads(response.text)
            genres = []
            for i in x["genres"]:
                genres.append(i["name"])
            if type == "Movie":
                return x["runtime"],genres
            elif type == "TV":
                return genres
        else:
            print("Error")
            return

def write_dictionary(link):
    if "imdb" in link.get('href'):
        query = "https://api.themoviedb.org/3/find/tt" + re.findall('\d+',link.get('href'))[0]
        params = {
            "api_key" : MOVIEDB_API_KEY,
            "language" : "en-US",
            "external_source" : "imdb_id"
        }
        response = requests.get(query,params=params)
        if response.status_code == 200:
            x = json.loads(response.text)
            if not x["movie_results"] and not x["person_results"] and not x["tv_results"]:
                ErrorsDF.loc[ErrorsDF.shape[0]] = [link.string,link.get('href')]
            elif not x["person_results"] and not x["tv_results"]:
                for i in x["movie_results"]:
                    details = get_details(i["id"],"Movie")
                    MoviesDF.loc[MoviesDF.shape[0]] = [i["title"],link.string,link.get('href'),str(i["vote_average"]),details[0],details[1]]
            elif not x["person_results"] and not x["movie_results"]:
                for i in x["tv_results"]:
                    details = get_details(i["id"],"TV")
                    TV_ShowsDF.loc[TV_ShowsDF.shape[0]] = [i["name"],link.string,link.get('href'),str(i["vote_average"]),details]
            elif not x["movie_results"] and not x["tv_results"]:
                return
        else:
            print("Error")
    else:
        Non_IMDB_URLsDF.loc[Non_IMDB_URLsDF.shape[0]] = [link.get('href')]

urls = soup.findAll('a')

with tqdm(total=len(urls)) as pbar:
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(write_dictionary,url) for url in urls]
        for future in as_completed(futures):
            result = future.result()
            pbar.update(1)

MoviesDF = MoviesDF.drop_duplicates(subset='Title',keep="first")
TV_ShowsDF = TV_ShowsDF.drop_duplicates(subset='Title',keep="first")
Non_IMDB_URLsDF.drop_duplicates()
ErrorsDF.drop_duplicates()

with pd.ExcelWriter('output.xlsx') as writer:  
    MoviesDF.to_excel(writer, sheet_name='Movies',index=False)
    TV_ShowsDF.to_excel(writer, sheet_name='TVShows',index=False)
    Non_IMDB_URLsDF.to_excel(writer, sheet_name='NonIMDbUrls',index=False)
    ErrorsDF.to_excel(writer, sheet_name='Errors',index=False)
    #PeopleDF.to_excel(writer, sheet_name='People',index=False)