# -*- coding: utf-8 -*-
"""BusinessExitsScraper.py

A script to scrape business listings from BusinessExits.com
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
import openai
import re

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
        'businessexits.com': {
            "limit": 10,  # Rate limiting for Business Exits
        },
    }
}

def get_region_from_state(state, openai_key):
    """Convert state to region using OpenAI"""
    if state != "all":
        client = openai.OpenAI(api_key=openai_key)
        
        prompt = f"""Given the US state '{state}', return the most applicable region from this list:
        ["mid-atlantic", "midwest", "nationwide-remote", "north-west", "northeast", "southeast", "southwest", "west", "west-coast"]
        
        Return ONLY the region name, nothing else."""
        
        try:
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that maps US states to regions."},
                    {"role": "user", "content": prompt}
                ]
            )
            return response.choices[0].message.content.strip().lower()
        except Exception as e:
            logger.error(f"Error getting region from OpenAI: {e}")
            return "all"
    else:
        return "all"

def runRequest(url, settings, scraperapi=False):
    if scraperapi:
        start_time = time.perf_counter()
        payload = {'api_key': '5a4fe9277d64dee6d98516137342135c', 'url': url}
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

def dispatch(url, region, settings, filepath, page=1):
    response, status, elapsed_time = runRequest(url, settings)

    if status != 200:
        return dispatch(url, region, settings, filepath, page)
    else:
        logger.info(
            msg=f"status={status}, url={url}",
            extra={
                "elapsed_time": f"{elapsed_time:.4f}",
            }
        )

        dir = f"./{filepath}"
        os.makedirs(dir, exist_ok=True)
        idx = url.split("?region=")[-1] if "?region=" in url else "all"
        filename = f"businessexits-{idx}-page{page}.html"
        loc = f"{dir}/{filename}"

        with open(loc, mode="w", encoding="utf-8") as fd:
            fd.write(response.text)
        return filename

def has_next_page(filepath, current_page):
    """Check if there is a next page link in the HTML."""
    with open(filepath, "r", encoding="utf-8") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")
    # Look for a pagination link to the next page
    # The next page number is current_page + 1
    next_page_str = str(current_page + 1)
    # Look for an <a> with the next page number
    pagination = soup.find_all('a')
    for a in pagination:
        if a.text.strip() == next_page_str:
            return True
    return False

def main(region, settings, filepath):
    start_urls = []
    page = 1
    all_filenames = []

    while True:
        if region == "all":
            if page == 1:
                url = "https://businessexits.com/listings/"
            else:
                url = f"https://businessexits.com/listings/?wpv_paged={page}&wpv_aux_current_post_id=13887&wpv_aux_parent_post_id=13887&wpv_sort_orderby=field-sorting_price&wpv_sort_order=desc&wpv_sort_orderby_as=numeric&wpv_view_count=14338"
        else:
            if page == 1:
                url = f"https://businessexits.com/listings/?region={region}"
            else:
                url = f"https://businessexits.com/listings/?region={region}&wpv_paged={page}&wpv_aux_current_post_id=13887&wpv_aux_parent_post_id=13887&wpv_sort_orderby=field-sorting_price&wpv_sort_order=desc&wpv_sort_orderby_as=numeric&wpv_view_count=14338"

        start_urls.append(url)
        filename = dispatch(url, region, settings, filepath, page)
        all_filenames.append(filename)

        # Check if there is a next page link
        fullpath = os.path.join(filepath, filename)
        if not has_next_page(fullpath, page):
            break
        page += 1

    print(f"Total requests: {len(start_urls)}")
    return all_filenames

def scrape_file(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all listing cards (key-listings)
    listings = soup.find_all('div', class_='key-listings')

    data = []
    for listing in listings:
        try:
            # URL (from parent <a>)
            parent_a = listing.find_parent('a')
            url = parent_a['href'] if parent_a and parent_a.has_attr('href') else ''
            if url and not url.startswith('http'):
                url = 'https://businessexits.com' + url

            # Name
            name_tag = listing.find('div', class_='listing_title')
            name = name_tag.text.strip() if name_tag else ''

            # Price
            price_tag = listing.find('div', class_='listing_price')
            price = price_tag.text.strip() if price_tag else ''

            # Revenue
            revenue_tag = listing.find('div', class_='listing_revenue')
            revenue = revenue_tag.text.strip() if revenue_tag else ''

            # Income
            income_tag = listing.find('div', class_='listing_income')
            income = income_tag.text.strip() if income_tag else ''

            # Type/Category
            type_tag = listing.find('div', class_='listing_type')
            type_ = type_tag.text.strip() if type_tag else ''

            # Status/Label
            label_tag = listing.find('div', class_='listing_label')
            label = label_tag.text.strip() if label_tag else ''

            # No description or location in card
            description = ''
            location = ''

            listing_data = {
                "name": name,
                "url": url,
                "price": price,
                "revenue": revenue,
                "income": income,
                "type": type_,
                "status": label,
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

def clean_numeric_value(value):
    """Extract numeric value from a string like 'Listing Price: $1,100,000'."""
    if not value:
        return value
    # Remove non-numeric characters except decimal point
    numeric_str = ''.join(c for c in value if c.isdigit() or c == '.')
    try:
        return float(numeric_str)
    except ValueError:
        return value

def get_data(combined_list, region):
    data = []
    for entry in combined_list:
        data.append({
            "Name": entry.get("name"),
            "URL": entry.get("url"),
            "Price": clean_numeric_value(entry.get("price")),
            "Revenue": clean_numeric_value(entry.get("revenue")),
            "Income": clean_numeric_value(entry.get("income")),
            "Type": entry.get("type"),
            "Status": entry.get("status"),
            "Description": entry.get("description"),
            "Location": entry.get("location"),
            "Region": region
        })
    df = pd.DataFrame(data)
    return df

def scrape_businessexits(state, settings, filepath, run_scrape, openai_key):
    if run_scrape:
        region = get_region_from_state(state, openai_key)
        all_filenames = main(region, settings, filepath)
    else:
        region = get_region_from_state(state, openai_key)
        all_filenames = [f for f in os.listdir(f'./{filepath}') if f.startswith(f'businessexits-{region}') or (region == 'all' and f.startswith('businessexits-all'))]

    files = get_files(filepath, all_filenames)
    combined_list = [item for sublist in files for item in sublist]
    businessexits_df = get_data(combined_list, region)
    return businessexits_df

# =====================
# EXTRA DETAIL SCRAPING FOR INDIVIDUAL BUSINESS PAGES
# =====================

def scrape_business_detail_extra(url, sleep_time=1):
    import requests
    from bs4 import BeautifulSoup
    import time
    import re

    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        time.sleep(sleep_time)
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return {"URL": url, "Description": '', "SDE": '', "Multiple": '', "Currency": 'USD', "EBITDA": ''}

    # --- Description ---
    description = ''
    desc_header = soup.find('h3', class_='business-description')
    if desc_header:
        desc_paragraphs = []
        for sib in desc_header.find_next_siblings():
            if sib.name == 'p':
                desc_paragraphs.append(sib.get_text(separator=' ', strip=True))
            elif sib.name and sib.name.startswith('h'):
                break
            elif sib.name == 'table':
                break
        description = '\n'.join(desc_paragraphs)

    # --- Financial Section: Multiple, Currency ---
    multiple = ''
    currency = ''
    # Find the Financial section
    financial_section = None
    for tag in soup.find_all(['h2', 'h3', 'h4']):
        if 'financial' in tag.get_text(strip=True).lower():
            financial_section = tag.find_parent() or tag.find_next_sibling()
            break
    if not financial_section:
        financial_section = soup

    text = financial_section.get_text(separator='\n', strip=True)
    # Multiple
    match = re.search(r'Multiple: *([\d\.]+x)', text)
    if match:
        multiple = match.group(1)
    # Currency from Listing Price
    match = re.search(r'Listing Price: *([\$€]?[\d,]+)', text)
    if match:
        price_val = match.group(1)
        if '$' in price_val:
            currency = 'USD'
        elif '€' in price_val:
            currency = 'EUR'
        elif 'CAD' in price_val or 'CAN' in price_val:
            currency = 'CAD'
        elif 'SAR' in price_val:
            currency = 'SAR'
    if not currency:
        currency = 'USD'

    # --- SDE/Earnings/EBITDA from Table ---
    sde = ''
    ebitda = ''
    table = soup.find('table')
    if table:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue
            label = cells[0].get_text(strip=True).lower()
            if 'sde' in label or 'earnings' in label:
                sde = cells[-1].get_text(strip=True)
            if 'ebitda' in label:
                ebitda = cells[-1].get_text(strip=True)

    return {
        "URL": url,
        "Description": description,
        "SDE": sde,
        "Multiple": multiple,
        "Currency": currency,
        "EBITDA": ebitda
    }

def scrape_all_details(url_list, sleep_time=1):
    import pandas as pd
    results = []
    for url in url_list:
        print(f"Scraping extra details: {url}")
        try:
            data = scrape_business_detail_extra(url, sleep_time)
            results.append(data)
        except Exception as e:
            print(f"Failed to scrape {url}: {e}")
    return pd.DataFrame(results)



if __name__ == "__main__":
    # Example usage
    state = "all"
    openai_key = "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"
    run_scrape = True
    final_df = scrape_businessexits(state, settings, 'businessexits-listings', run_scrape, openai_key)
    final_df.to_csv(f"businessexits_{state.lower()}_listings.csv", index=False) 


    main_df = pd.read_csv(f"businessexits_{state.lower()}_listings.csv")
    url_list = main_df['URL'].dropna().unique().tolist()
    details_df = scrape_all_details(url_list)
    final_df = pd.merge(main_df, details_df, on='URL', how='left')
    final_df.to_csv(f"businessexits_{state.lower()}_details_enriched.csv", index=False)
    print(final_df)