import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
import zlib
import json
import sys
from dotenv import load_dotenv
import os


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")

def get_movie_ids(date: str, top=10000):
    year, month, day = date.split("-")
    load_data = requests.get(f"http://files.tmdb.org/p/exports/movie_ids_{month}_{day}_{year}.json.gz")
    if load_data.status_code == 200:
        print("Success")
        decompressed_data=zlib.decompress(load_data.content, 16+zlib.MAX_WBITS)
        movies_pop = [json.loads(i) for i in decompressed_data.decode("utf-8").split("\n") if i != ""]
        sort_movie_ids = sorted(movies_pop, key=lambda x: x['popularity'], reverse=True)
        top_movie_ids = [i["id"] for i in sort_movie_ids[:top]]
        return  top_movie_ids
    else:
        return None

async def extract_movie_details_async(movie_id, sem):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    print(API_KEY)
    async with sem:
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    json_data = await response.json()
                    return json_data
        except:
            return {"id": movie_id, "popularity": None} 

async def extract_movie_credits_async(movie_id, sem):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?language=en-US"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    async with sem:
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    json_data = await response.json()
                    return json_data
        except:
            return {"id": movie_id, "cast": None}

async def extract_box_office_data_async(imdb_id, sem):
    box_office_url = f"https://www.boxofficemojo.com/title/{imdb_id}/"

    async with sem:
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                async with session.get(box_office_url) as response:
                    response.raise_for_status()
                    text = await response.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    extract_summary = soup.find("div", {"class": "mojo-summary-table"})
                    extract_performance = extract_summary.find("div", {"class":"mojo-performance-summary-table"}).find_all("div", {"class":"a-section"})

                    worldwide_gross = extract_performance[-1].find("span", {"class":"a-size-medium"}).text.replace("\n","").strip()
                    find_data = extract_summary.find("div", {"class":"mojo-summary-values"}).find_all("div", {"class":"a-section"})

                    budget = ''
                    domestic_opening = ''
                    for row in find_data:
                        if "Budget" in row.text:
                            budget = row.text.replace("\n","").strip().split("$")[-1]

                        if "Domestic Opening" in row.text:
                            domestic_opening = row.text.replace("\n","").strip().split("$")[-1]
                    return {
                            "imdb_id": imdb_id,
                            "domestic_opening" : domestic_opening,
                            "worldwide_gross": worldwide_gross,
                            "budget": budget
                        }
        except:
            return {
                            "imdb_id": imdb_id,
                            "domestic_opening": '',
                            "worldwide_gross": '',
                            "budget": ''
                        }


async def extract_movie_details(movie_ids):
    sem = asyncio.Semaphore(100)
    movies_details_tasks = [extract_movie_details_async(movie_id,sem) for movie_id in movie_ids]
    movies_details = await asyncio.gather(*movies_details_tasks)

    fail_ids = [movie for movie in movies_details if movie['popularity']==None]
    movies_details_tasks_fail = [extract_movie_details_async(page["id"],sem) for page in fail_ids]
    movies_details_fail = await asyncio.gather(*movies_details_tasks_fail)

    complete_movies_detail = [movie for movie in movies_details if movie['popularity']!=None]+[movie for movie in movies_details_fail if movie['popularity'] != None]
    return complete_movies_detail

async def extract_movie_credits(movie_ids):
    sem = asyncio.Semaphore(100)
    movies_credits_tasks = [extract_movie_credits_async(movie_id,sem) for movie_id in movie_ids]
    movies_credits = await asyncio.gather(*movies_credits_tasks)

    fail_ids = [movie for movie in movies_credits if movie['cast']==None]
    movies_credits_tasks_fail = [extract_movie_credits_async(page["id"],sem) for page in fail_ids]
    movies_credits_fail = await asyncio.gather(*movies_credits_tasks_fail)

    complete_movies_credits = [movie for movie in movies_credits if movie['cast']!=None]+[movie for movie in movies_credits_fail if movie['cast'] != None]

    return complete_movies_credits

async def extract_box_office_data(imdb_ids):
    sem = asyncio.Semaphore(50)
    box_office_tasks = [extract_box_office_data_async(imdb_id,sem) for imdb_id in imdb_ids]
    box_office_data = await asyncio.gather(*box_office_tasks)

    fail_box_id = [i["imdb_id"] for i in box_office_data if i["worldwide_gross"] == ""]
    box_office_data_tasks_fail = [extract_box_office_data_async(page,sem) for page in fail_box_id]
    box_office_data_tasks_fail = await asyncio.gather(*box_office_data_tasks_fail)

    complete_box_office_data = [i for i in box_office_data if i["worldwide_gross"] != ""] + box_office_data_tasks_fail

    return complete_box_office_data

def run_movie_details(movie_ids):
    return asyncio.run(extract_movie_details(movie_ids))

def run_movie_credits(movie_ids):
    return asyncio.run(extract_movie_credits(movie_ids))

def run_box_office_data(imdb_ids):
    return asyncio.run(extract_box_office_data(imdb_ids))