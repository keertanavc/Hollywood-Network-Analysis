''' Program to retrieve, clean, and store OMDb data '''

from bs4 import BeautifulSoup
import requests
import urllib.request
import re
import omdb
import json
import pandas as pd
import numpy as np

def main():
    # Retrive IDs
    ids_filename = 'ids'
    get_ids(ids_filename)
    ids = pd.read_csv(ids_filename)
    ids = ids['0'].values.tolist()

    # Gather data
    use_ids = ids[use_ids_start:use_ids_end]
    output_filename = 'data_d10.json'
    API_KEYS = list(pd.read_csv('keys.csv', header=False))
    API_KEY = API_KEYS[0]
    omdb_api_call(use_ids, API_KEY, output_filename)

    # Concatenate and clean the data + save as pandas dataframe
    json_final_file = 'omdb_data.json'
    concatenate_data(json_final_file)
    with open(json_final_file) as json_file:
        data = json.load(json_file)
    pandas_file = 'cleaned_data.csv'
    data_clean_transform(data, pandas_file)


def data_clean_transform(data, filename):
    ''' 
    Cleans data and converts to a pandas dataframe 
    Inputs:
    data: list of dictionaries of movies
    filename: file name to store pandas dataframe
    Output: nothing

    df cleaning: (1) remove repeating id entries
    '''
    cols1 = list(data[0].keys())
    cols1 = [c for c in cols1 if c != 'ratings']
    cols2 = [i['source'] for i in data[0]['ratings']]
    df = pd.DataFrame(columns=cols1+cols2)
    ids_seen = set()
    ind = 0

    for movie in data:
        print(ind)
        if not movie or movie['imdb_id'] in ids_seen:
            continue
        ids_seen.add(movie['imdb_id'])      
        ratings_vals = []
        for j in cols2:
            flag = 0
            for i in movie['ratings']:
                if i['source'] == j:
                    ratings_vals.append(i['value'])
                    flag = 1
            if flag == 0:
                ratings_vals.append('NA')

        df.loc[ind] = [movie[c] for c in cols1] + ratings_vals
        ind = ind + 1

    # removing release contries that are not US or are nan values
    # removing TV mini-series, documentaries etc.
    u = []
    for j in range(df.shape[0]):
        i = df['country'].loc[j]
        k = df['type'].loc[j]
        val = False
        if not pd.isnull(i):
            if ('USA' in i):
                #print(i)
                val = True
        if val:
            if k in ['episode', 'series']:
                val = False
        u.append(val)
    df = df[u]

    # dropping unwanted columns
    remove_cols =   ['Unnamed: 0', 'plot', 'language', 'country', 'poster', 'type', 
                'tomato_meter', 'tomato_image', 'tomato_rating', 'tomato_reviews',
                'tomato_fresh', 'tomato_rotten', 'tomato_consensus',
                'tomato_user_meter', 'tomato_user_rating', 'tomato_user_reviews',
                'tomato_url', 'dvd', 'website', 'response', 'imdb_votes', 'box_office']
    df = df.drop(columns=remove_cols)

    # converting columns to the right dtype
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    u = [np.nan if r in ['Not Rated', 'Unrated', 'NOT RATED', 'UNRATED', 'NR']\
        else r for r in df['rated']]
    df['rated'] = u
    df['released'] = pd.to_datetime(df['released'])
    u = [np.nan if pd.isnull(r) else int(r[:-4]) for r in df['runtime']]
    df['runtime'] = u

    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['metascore'] = pd.to_numeric(df['metascore'], errors='coerce')
    df['imdb_rating'] = pd.to_numeric(df['imdb_rating'], errors='coerce')
    # Not using these because of sparsity in data
    #df['imdb_votes'] = pd.to_numeric(df['imdb_votes'], errors='coerce')
    #df['box_office'] = df['box_office'].replace('[\$,]', '', regex=True)
    #df['box_office'] = pd.to_numeric(df['box_office'], errors='coerce')
    df['Internet Movie Database'] = df['Internet Movie Database'].replace('/10', '', regex=True)
    df['Internet Movie Database'] = pd.to_numeric(df['Internet Movie Database'], errors='coerce')
    df['Rotten Tomatoes'] = df['Rotten Tomatoes'].replace('%', '', regex=True)
    df['Rotten Tomatoes'] = pd.to_numeric(df['Rotten Tomatoes'], errors='coerce')
    df['Metacritic'] = df['Metacritic'].replace('/100', '', regex=True)
    df['Metacritic'] = pd.to_numeric(df['Metacritic'], errors='coerce')
    df.to_csv(filename, index=False)

    # Removing some rows with NA values
    df = df[df['writer'].notnull()]
    df = df[df['actors'].notnull()]
    df = df[df['director'].notnull()]
    
    # Accounting for different movies with same title
    # Making up for multiple title names
    unique_movies = set()
    for i, row in df['title'].iteritems():
        if row not in unique_movies:
            unique_movies.add(row)
        else:
            row2 = row + '2'
            if row2 not in unique_movies:
                unique_movies.add(row2)
                df.loc[i, 'title'] = row2
            else:
                row3 = row + '3'
                if row3 not in unique_movies:
                    unique_movies.add(row3)
                    df.loc[i, 'title'] = row3
                else:
                    row4 = row + '4'
                    if row4 not in unique_movies:
                        unique_movies.add(row4)
                        df.loc[i, 'title'] = row4
                    else:
                        print('More than 4 movies with same name!', row, i)

    df.to_csv(filename, index=False)


def concatenate_data(outputfile):
    ''' Function to concatenate all sub-data files into single outputfile'''
    final_data = []
    for i in range(10):
        file = 'data_d' + str(i+1) + '.json'
        print(i+1)
        with open(file) as json_file:
            data = json.load(json_file)
        final_data = final_data + data
    with open(outputfile, 'w') as fout:
        json.dump(final_data, fout)


def omdb_api_call(use_ids, API_KEY, output_filename):
    '''
    Function to call the OMDb API to retrive information given ids
    
    Inputs:
    use_ids: list of int: list of IMDb ids for which info has to be retrieved
    API_KEY: the API key used for call
    output_filename: file in which the data has to be stored

    Returns: Nothing
    '''
    data_list = []
    omdb.set_default('apikey', API_KEY)
    for i in range(len(use_ids)):
        print(i)
        data_list.append(omdb.imdbid(use_ids[i], fullplot=True, tomatoes=True))
    with open(output_filename, 'w') as fout:
        json.dump(data_list, fout)


def get_ids(ids_filename):
    ''' 
    Scrapes ids of all movies from the list https://www.imdb.com/list/ls057823854/
    Input: ids_filename: file into which the IDs have to be stored
    Returns: Nothing
    '''
    url = 'https://www.imdb.com/list/ls057823854/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    a = soup.find_all("script", type="application/ld+json")[0]
    ids_raw = re.findall('"/title/tt\d.+/"',a.text)
    ids = []
    for i in ids_raw:
        m = re.search('"/title/(.+?)/"', i)
        if m:
            ids.append(m.group(1))
    ids = pd.DataFrame(np.array(ids))
    ids.to_csv(ids_filename, index=False)

def bechdel_test_scraper(df):
    df['bechdel'] = 0
    df.index
    t = 0
    for i, row in df.iterrows():
        print(i)
        if i > 30:
            break
        t = t+1
        print(t)
        ID = row['imdb_id'][2:]
        link = 'http://bechdeltest.com/api/v1/getMovieByImdbId?imdbid=' + ID
        response = requests.get(link)
        if response.status_code != 200:
            print(title)
            continue
        df.loc[i,'bechdel'] = response.json()['rating']

def add_box_office(df):    
    # Adding box office information
    box_office = pd.read_csv('box_office_data.csv')
    other = box_office[['Title', 'Production budget', 'Worldwide Gross']]
    other.columns = ['title', 'prod_bud', 'world_gross']
    r = [not i for i in other['title'].duplicated()]
    other = other[r]
    x = df.merge(other, on='title', how='left',suffixes=(False, False))

def gender_finder(names, filename):
    ''' Finds the genders for names and stores in filename '''

    d = gender.Detector()
    genders = [d.get_gender(a.split(" ")[0]) for a in names]
    d = pd.DataFrame([names, genders]).T
    d.to_csv(filename, index=False)
