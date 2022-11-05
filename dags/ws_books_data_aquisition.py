from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator,ShortCircuitOperator
from airflow.operators.bash import BashOperator
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import re
import json



def get_hrefs(URL_TO_AQUISITION_DATA='http://books.toscrape.com/')->list :

    '''
        This method make a list with hrefs of category of books.

        Parameters : 
            URL_TO_AQUISITION_DATA : A string with URL to get data

        Example : 
            get_hrefs(URL_TO_AQUISITION_DATA)
        Returns : 
            ['catalogue/category/books_1/index.html',
            'catalogue/category/books/travel_2/index.html',
            'catalogue/category/books/mystery_3/index.html' ...]
    '''
    list_hrefs = list()
    response = requests.get(URL_TO_AQUISITION_DATA)
    soup = bs(response.content,features='html.parser')
    data_ = soup.find('ul', class_='nav nav-list')
    # looping to take hrefs
    for line in data_.find_all('li') :
        dict_ = line.find('a', href=True)
        list_hrefs.append(dict_['href'])
    
    # Ignore first position 'cause a home page
    return list_hrefs[1:]

def get_books_genre (soup) : 
    '''
        This method get genre of book
    '''
    genre = soup.find('title').text.split("|")[0].strip()

    return genre

def get_book_genre_and_names(soup) :
    '''
        This method get genre for all books and
        your names.
    '''
    list_names = list()
    list_genre = list()
    genre = get_books_genre(soup)
    for name in soup.find_all('img',alt=True) :
        list_names.append(name['alt'])
        list_genre.append(genre)
    return list_genre,list_names

def get_book_prices(soup) :
    '''
        This method get price of book.
    '''
    list_prices = list()
    for price in soup.find_all(class_='price_color') :
        list_prices.append(float(price.text[1:]))
    return list_prices

def get_book_ratings(soup) :
    '''
        This method get rating of books
    '''
    list_rating = list()
    for rating in soup.find_all('p') :
        rating = str(rating)
        if re.search('star-rating',rating) : 
            span = re.search('star-rating',rating)
            end_word = span.span()[1]
            rating = rating[end_word:rating.index('>')].replace('"','').strip()
            list_rating.append(rating)
        
    return list_rating

def get_book_in_stock(soup) : 
    '''
        This method get status of book
    '''
    list_status = list()
    for status_ in soup.find_all(class_='instock availability') :
        list_status.append((status_.text.strip()))

    return list_status

def get_features_books(ti) :
    '''
        This method get all features of all pages and books

        Parameters : 
            URL_TO_AQUISITION_DATA : A URL to data aquisition
            list_hrefs : A list of hrefs from genres
    '''
    # Instance lists to reception data
    list_genres = list()
    list_names = list()
    list_prices = list()
    list_ratings = list()
    list_status = list()

    # Getting return of before task
    list_hrefs = ti.xcom_pull(task_ids = 'getting_hrefs')
    
    for href in list_hrefs :
        
        response = requests.get('http://books.toscrape.com/'+href)
        soup = bs(response.content, features='html.parser')
        # Get genres and names of books
        genres,names = get_book_genre_and_names(soup)
        # Get prices of books
        prices = get_book_prices(soup)
        # Get rating of books
        ratings = get_book_ratings(soup)
        # Get status of stock book
        status = get_book_in_stock(soup)

        # Add data to lists
        list_genres.extend(genres)
        list_names.extend(names)
        list_prices.extend(prices)
        list_ratings.extend(ratings)
        list_status.extend(status)
        

        # Verifying if exists more pages from genre
        if soup.find(class_='current') :
            qtd_pages = int(soup.find(class_='current').text.strip()[-1])

            for page in range(2,qtd_pages+1) :
                str_ = f"page-{page}.html"
                href_ = href.replace('index.html',str_)
                response = requests.get('http://books.toscrape.com/'+href_)
                soup = bs(response.content, features='html.parser')
                # Get names of books
                genres, names = get_book_genre_and_names(soup)
                # Get prices of books
                prices = get_book_prices(soup)
                # Get rating of books
                ratings = get_book_ratings(soup)
                # Get status of stock book
                status = get_book_in_stock(soup)

                # Add data to lists
                list_genres.extend(genres)
                list_names.extend(names)
                list_prices.extend(prices)
                list_ratings.extend(ratings)
                list_status.extend(status)

        else :
            continue


    # Transform in a dataframe
    data_ = pd.concat([pd.Series(list_genres), pd.Series(list_names), 
    pd.Series(list_prices), pd.Series(list_ratings),
    pd.Series(list_status)],axis=1).rename(
        columns={
            0:'genre',1:'names',2:'prices',
            3:'ratings',4:'stock_status'})

    return data_.to_json()

def save_csv_file (ti) :
    dataframe = ti.xcom_pull(task_ids = 'creating_dataframe')
    dict_ = json.loads(dataframe)
    dataframe = pd.DataFrame.from_dict(dict_)
    dataframe.to_csv(
        f"/opt/airflow/data/{today.strftime('%y')}.{today.strftime('%m')}.{today.strftime('%d')}_dataframe_books.csv",index=False)

# Args to DAG   
today = datetime.now()
default_args = {
        'start_date': today,
        'schedule_interval': '0 12 * * SUN',
        'catchup': False,
}

with DAG(dag_id='ws_books_data_aquisition', default_args=default_args) as dag :

    getting_hrefs = PythonOperator(
        task_id = 'getting_hrefs',
        python_callable=get_hrefs,
    )
    creating_dataframe = PythonOperator(
        task_id = 'creating_dataframe',
        python_callable=get_features_books,
    )
    saving_csv = PythonOperator(
        task_id='saving_csv',
        python_callable=save_csv_file,
    )

getting_hrefs >> creating_dataframe >> saving_csv