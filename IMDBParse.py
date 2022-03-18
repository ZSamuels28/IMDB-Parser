from bs4 import BeautifulSoup
import json
import requests
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import tkinter as tk
#import tkinter
from tkinter.filedialog import askopenfilename
import os

MOVIEDB_API_KEY = os.environ.get('MOVIEDB_API_KEY')

MoviesDF = pd.DataFrame(columns=['Title','IMDB URL','IMDB Rating','Duration','Genres'])
TV_ShowsDF = pd.DataFrame(columns=['Title','IMDB URL','IMDB Rating','Genres'])
Non_IMDB_URLsDF = pd.DataFrame(columns=['URL'])
ErrorsDF = pd.DataFrame(columns=['Input URL'])
#PeopleDF = pd.DataFrame()

print("Please select an HTML or TXT file")

application_window = tk.Tk()
application_window.withdraw() # we don't want a full GUI, so keep the root window from appearing
my_filetypes = [('Valid Files', '.txt .html'), ('Text Files', '.txt',), ('HTML Files', '.html')]
filename = askopenfilename(initialdir=os.getcwd(),title="Please select a file:",filetypes=my_filetypes) # show an "Open" dialog box and return the path to the selected file

if filename.lower().endswith('.html'):
    with open(filename) as fp:
        soup = BeautifulSoup(fp, "html.parser")
        urls = []
        for i in soup.findAll('a'):
            urls.append(i.get('href'))
elif filename.lower().endswith('.txt'):
    urls = [line.strip() for line in open(filename)]
else:
    print("File extension error")
    exit()

def get_details(id,type):
        query = "https://api.themoviedb.org/3/" + type + "/" + str(id)
        params = {
            "api_key" : MOVIEDB_API_KEY,
            "language" : "en-US"
        }
        response = requests.get(query,params=params)
        if response.status_code == 200:
            x = json.loads(response.text)
            genres = []
            for i in x.get("genres"):
                genres.append(i.get("name"))
            if type == "movie":
                return x.get("runtime"),genres
            elif type == "tv":
                return genres
            else:
                print("Type Not Found")
                return
        else:
            print("Error")
            return

def write_dictionary(link):
    if "imdb" in link:
        query = "https://api.themoviedb.org/3/find/tt" + re.findall('\d+',link)[0]
        params = {
            "api_key" : MOVIEDB_API_KEY,
            "language" : "en-US",
            "external_source" : "imdb_id"
        }
        response = requests.get(query,params=params)
        if response.status_code == 200:
            x = json.loads(response.text)
            if not x.get("movie_results") and not x.get("person_results") and not x.get("tv_results"):
                ErrorsDF.loc[ErrorsDF.shape[0]] = [link]
            elif not x.get("person_results") and not x.get("tv_results"):
                for i in x.get("movie_results"):
                    details = get_details(i.get("id"),"movie")
                    MoviesDF.loc[MoviesDF.shape[0]] = [i.get("title"),link,str(i.get("vote_average")),details[0],details[1]]
            elif not x.get("person_results") and not x.get("movie_results"):
                for i in x.get("tv_results"):
                    details = get_details(i.get("id"),"tv")
                    TV_ShowsDF.loc[TV_ShowsDF.shape[0]] = [i.get("name"),link,str(i.get("vote_average")),details]
            elif not x.get("movie_results") and not x.get("tv_results"):
                return
        else:
            print("Error")
    else:
        Non_IMDB_URLsDF.loc[Non_IMDB_URLsDF.shape[0]] = [link]

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