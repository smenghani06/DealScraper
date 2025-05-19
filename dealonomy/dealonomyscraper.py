# -*- coding: utf-8 -*-
"""DealonomyDealScraper.py

A script to scrape business listings from Dealonomy.com
"""

from pythonjsonlogger import jsonlogger
from urllib.parse import urlparse
import requests
import logging
import time
import random
import os
import math
import pandas as pd
from bs4 import BeautifulSoup
import json
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

settings = {
    "max_tcp_connections": 1,
    "proxies": [
        "http://localhost:8765",
    ],
    "rate_per_host": {
        'www.dealonomy.com': {
            "limit": 10,  # Rate limiting for Dealonomy
        },
    }
}

def runRequest(url, settings, scraperapi=False):
    if (scraperapi):
        start_time = time.perf_counter()
        payload = { 'api_key': '5a4fe9277d64dee6d98516137342135c', 'url': url }
        response = requests.get('https://api.scraperapi.com/', params=payload)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        status = response.status_code
        return(response, status, elapsed_time)
    else:
        host = urlparse(url).hostname
        max_tcp_connections = settings['max_tcp_connections']

        safari_agents = [
            'Safari/17612.3.14.1.6 CFNetwork/1327.0.4 Darwin/21.2.0',
        ]
        user_agent = random.choice(safari_agents)

        headers = {
            'User-Agent': user_agent
        }

        proxy = None
        start_time = time.perf_counter()
        response = requests.get(url, headers=headers, proxies={'http': proxy, 'https': proxy})
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        status = response.status_code
        return(response, status, elapsed_time)

def dispatch(url, state, settings, filepath):
    response, status, elapsed_time = runRequest(url, settings)

    if status != 200:
        dispatch(url, state, filepath, settings)
    else:
        logger.info(
            msg=f"status={status}, url={url}",
            extra={
                "elapsed_time": f"{elapsed_time:.4f}",
            }
        )

        dir = f"./{filepath}"
        os.makedirs(dir, exist_ok=True)
        idx = url.split(f"https://www.dealonomy.com/s?states={state}&page=")[-1]
        loc = f"{dir}/dealonomy-{state.lower()}-{idx}.html"

        with open(loc, mode="w", encoding="utf-8") as fd:
            fd.write(response.text)

def main(state, settings, filepath):
    start_urls = []
    i = 1
    stop = False
    
    # Initial request to get total pages
    base_url = f"https://www.dealonomy.com/s?states={state}"
    response, status, elapsed_time = runRequest(base_url, settings)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find total number of pages (this selector might need adjustment based on actual HTML structure)
    try:
        pagination = soup.find('div', class_='pagination')
        page_count = int(pagination.find_all('a')[-2].text)
    except:
        page_count = 1

    while not stop:
        url = f"{base_url}&page={i}"
        start_urls.append(url)
        dispatch(url, state, settings, filepath)

        if i == page_count:
            stop = True
        else:
            i += 1
            
    print(f"Total requests: {len(start_urls)}")

def scrape_file(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Find all listing cards (new selector)
    listings = soup.find_all('div', class_='text-brand-primary-navy shadow-sm transition-shadow duration-200 ease-in-out border-brand-gray-blueGray relative rounded-2xl border bg-white md:flex md:items-start')
    
    data = []
    for listing in listings:
        try:
            # Name
            name_tag = listing.find('h2', class_='text-xl font-bold md:text-2xl')
            name = name_tag.text.strip() if name_tag else ''

            # URL (find the first <a> with class 'absolute inset-0 z-10')
            url_tag = listing.find('a', class_='absolute inset-0 z-10')
            url = 'https://www.dealonomy.com' + url_tag['href'] if url_tag and url_tag.has_attr('href') else ''

            # Price (find <p> with class 'text-lg font-bold md:text-xl')
            price_tag = listing.find('p', class_='text-lg font-bold md:text-xl')
            price = price_tag.text.strip() if price_tag else ''

            # Description (find <p> with class 'mb-4 text-sm text-gray-700 md:mb-6')
            desc_tag = listing.find('p', class_='mb-4 text-sm text-gray-700 md:mb-6')
            description = desc_tag.text.strip() if desc_tag else ''

            # Location (find <div> with class 'mt-1 flex items-center text-gray-600', then get the <span> text)
            loc_div = listing.find('div', class_='mt-1 flex items-center text-gray-600')
            location = ''
            if loc_div:
                span = loc_div.find('span')
                location = span.text.strip() if span else ''

            listing_data = {
                "name": name,
                "url": url,
                "price": price,
                "description": description,
                "location": location
            }
            data.append(listing_data)
        except Exception as e:
            logger.error(f"Error parsing listing: {str(e)}")
            continue
            
    return data

def get_files(main_filepath, list_of_filepaths):
    list_of_files = []
    for filepath in list_of_filepaths:
        list_of_files.append(scrape_file(f"{main_filepath}/{filepath}"))
    return list_of_files

def combine_lists(list_of_files):
    combined_list = []
    for sublist in list_of_files:
        combined_list.extend(sublist)
    return combined_list

def get_data(combined_list, state):
    data = []
    for entry in combined_list:
        data.append({
            "Name": entry.get("name"),
            "Description": entry.get("description"),
            "URL": entry.get("url"),
            "Price": entry.get("price"),
            "Location": entry.get("location"),
            "State": state
        })
    df = pd.DataFrame(data)
    return df

def scrape_dealonomy(state, settings, filepath, run_scrape):
    if run_scrape:
        state = state.replace(' ', '-')
        state = state.title()
        main(state, settings, filepath)
    
    list_of_filepaths = os.listdir(f'./{filepath}')
    try:
        list_of_filepaths.remove('.ipynb_checkpoints')
    except:
        pass
        
    files = get_files(filepath, list_of_filepaths)
    combined_list = combine_lists(files)
    dealonomy_df = get_data(combined_list, state)
    return dealonomy_df

if __name__ == "__main__":
    # Example usage
    state = "Texas"
    run_scrape = True
    final_df = scrape_dealonomy(state, settings, 'dealonomy-listings', run_scrape)
    final_df.to_csv(f"dealonomy_{state.lower()}_listings.csv", index=False) 