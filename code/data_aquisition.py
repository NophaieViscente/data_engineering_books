import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re
import logging

# Constants
from decouple import config
PROCESSED_DATA_PATH     = config('PROCESSED_DATA_PATH')
RAW_PROCESSED_DATA_PATH = config('RAW_PROCESSED_DATA_PATH')
URL_TO_AQUISITION_DATA  = config('URL_TO_AQUISITION_DATA')
LOGS_PATH = config('LOGS_PATH')

# Creating logging
logging.basicConfig(level=logging.INFO, filename=LOGS_PATH+'log_scrapper.txt')
logger = logging.getLogger('scraper')

def get_hrefs(URL_TO_AQUISITION_DATA:str)->list :

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
    if response.raise_for_status() :
        print('código_quebrado')
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

def get_features_books(URL_TO_AQUISITION_DATA:str) :
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

    # Get hrefs of pages
    list_hrefs = get_hrefs(URL_TO_AQUISITION_DATA)
    
    for href in list_hrefs :
        
        response = requests.get(URL_TO_AQUISITION_DATA+href)
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
        
        # Saving log
        logger.info(f"Receiving data from: {href}")

        # Verifying if exists more pages from genre
        if soup.find(class_='current') :
            qtd_pages = int(soup.find(class_='current').text.strip()[-1])

            for page in range(2,qtd_pages+1) :
                str_ = f"page-{page}.html"
                href_ = href.replace('index.html',str_)
                response = requests.get(URL_TO_AQUISITION_DATA+href_)
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

                # Saving log
                logger.info(f"Receiving data from: {href_}")
        else :
            continue


    # Transform in a dataframe
    data_ = pd.concat([pd.Series(list_genres), pd.Series(list_names), 
    pd.Series(list_prices), pd.Series(list_ratings),
    pd.Series(list_status)],axis=1).rename(
        columns={
            0:'genre',1:'names',2:'prices',
            3:'ratings',4:'stock_status'})

    return data_

def main () : 
    get_features_books(URL_TO_AQUISITION_DATA).to_csv(PROCESSED_DATA_PATH+'dataframe_books_engineering.csv',index=False)

if __name__ == '__main__' :
    main()