#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AnimePahe RSS Feed Generator with Direct Download URLs
Generates an RSS feed for the latest 25 anime releases from AnimePahe with direct download links
Designed for Koyeb deployment with auto-updates every 30-60 seconds, maintaining 25 items with newest at top
"""
import logging
import os
import re
import time
import random
import asyncio
import json
import threading
import aiohttp
import requests
from datetime import datetime, timezone
import pytz
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import quote
from tenacity import retry, stop_after_attempt, wait_exponential
import xml.etree.ElementTree as ET
from xml.dom import minidom
import cloudscraper
from flask import Flask, send_file, Response
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('rss_generator.log')
    ]
)
logger = logging.getLogger(__name__)

# Flask app setup
app = Flask(__name__)

# Headers for requests
HEADERS = {
    'authority': 'animepahe.ru',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
    'cookie': '__ddg2_=;',
    'dnt': '1',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="124", "Chromium";v="124"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'x-requested-with': 'XMLHttpRequest',
    'referer': 'https://animepahe.ru/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
}

# Configuration
BASE_DIR = Path(__file__).parent.resolve()
RSS_FILE = BASE_DIR / "animepahe_feed.xml"
CACHE_FILE = BASE_DIR / "animepahe_cache.json"
QUALITY_PREFERENCES = ["360p", "720p", "1080p"]
UPDATE_INTERVAL_MIN = 30  # seconds
UPDATE_INTERVAL_MAX = 60  # seconds
MAX_ITEMS = 25  # Maximum number of items in RSS feed
MAX_WORKERS = 4  # Number of simultaneous processing

# Create directories
BASE_DIR.mkdir(parents=True, exist_ok=True)

# Global session with proper cookie handling
session = None
scraper = None
kwik_scraper = None  # Dedicated scraper for kwik/pahe links

def create_scraper():
    """Create a cloudscraper session with proper configuration"""
    global scraper
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
        interpreter='nodejs'
    )
    
    # Set headers
    scraper.headers.update(HEADERS)
    
    # Initialize session by visiting the main page
    try:
        response = scraper.get("https://animepahe.ru/")
        response.raise_for_status()
        logger.info("Cloudscraper session initialized successfully")
        time.sleep(random.uniform(1, 2))
    except Exception as e:
        logger.error(f"Failed to initialize cloudscraper session: {str(e)}")
    
    return scraper

def create_kwik_scraper():
    """Create a dedicated cloudscraper session for kwik/pahe links"""
    global kwik_scraper
    kwik_scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
        interpreter='nodejs'
    )
    
    # Set specific headers for kwik/pahe
    kwik_headers = {
        'authority': 'pahe.win',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    kwik_scraper.headers.update(kwik_headers)
    
    # Initialize session by visiting the main page
    try:
        response = kwik_scraper.get("https://pahe.win/")
        response.raise_for_status()
        logger.info("Kwik scraper session initialized successfully")
        time.sleep(random.uniform(1, 2))
    except Exception as e:
        logger.error(f"Failed to initialize kwik scraper session: {str(e)}")
    
    return kwik_scraper

def create_session():
    """Create a requests session with proper headers and cookies"""
    global session
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Initialize session by visiting the main page
    try:
        response = session.get("https://animepahe.ru/")
        response.raise_for_status()
        logger.info("Session initialized successfully")
        time.sleep(random.uniform(1, 2))
    except Exception as e:
        logger.error(f"Failed to initialize session: {str(e)}")
    
    return session

def load_cached_releases():
    """Load cached releases from disk if available"""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r') as f:
                data = json.load(f)
                logger.info("Loaded cached releases from disk")
                return data.get('releases', [])
        except Exception as e:
            logger.error(f"Error loading cache: {str(e)}")
    return []

def save_cached_releases(releases):
    """Save releases to disk for fallback"""
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump({'releases': releases}, f)
        logger.info("Saved releases to cache")
    except Exception as e:
        logger.error(f"Error saving cache: {str(e)}")

# Helper functions for kwik link processing
def step_2(s, seperator, base=10):
    """Step 2 of kwik link processing"""
    mapped_range = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+/"
    numbers = mapped_range[0:base]
    max_iter = 0
    for index, value in enumerate(s[::-1]):
        max_iter += int(value if value.isdigit() else 0) * (seperator**index)
    mid = ''
    while max_iter > 0:
        mid = numbers[int(max_iter % base)] + mid
        max_iter = (max_iter - (max_iter % base)) / base
    return mid or '0'

def step_1(data, key, load, seperator):
    """Step 1 of kwik link processing"""
    payload = ""
    i = 0
    seperator = int(seperator)
    load = int(load)
    while i < len(data):
        s = ""
        while data[i] != key[seperator]:
            s += data[i]
            i += 1
        for index, value in enumerate(key):
            s = s.replace(value, str(index))
        payload += chr(int(step_2(s, seperator, 10)) - load)
        i += 1
    payload = re.findall(
        r'action="([^\"]+)" method="POST"><input type="hidden" name="_token"\s+value="([^\"]+)', payload
    )[0]
    return payload

def get_dl_link(link):
    """Get direct download link from kwik link"""
    global kwik_scraper
    if kwik_scraper is None:
        kwik_scraper = create_kwik_scraper()
    
    try:
        # Add random delay to avoid rate limiting
        time.sleep(random.uniform(1, 2))
        
        # Set referer header
        local_headers = kwik_scraper.headers.copy()
        local_headers['referer'] = link
        
        # First request to get the page
        resp = kwik_scraper.get(link, headers=local_headers)
        
        # Extract the JavaScript code that contains the parameters
        pattern = r'\("(\S+)",\d+,"(\S+)",(\d+),(\d+)'
        match = re.search(pattern, resp.text)
        if not match:
            # Try alternative pattern
            pattern = r'\("([^"]+)",(\d+),"([^"]+)",(\d+),(\d+)'
            match = re.search(pattern, resp.text)
            if match:
                data, _, key, load, seperator = match.groups()
            else:
                return None
        else:
            data, key, load, seperator = match.groups()
        
        # Process the parameters
        url, token = step_1(data=data, key=key, load=load, seperator=seperator)
        
        # Prepare the POST request
        post_url = url if url.startswith('http') else f"https://kwik.si{url}"
        data = {"_token": token}
        headers = {'referer': link}
        
        # Make the POST request
        resp = kwik_scraper.post(url=post_url, data=data, headers=headers, allow_redirects=False)
        
        # Check if we have a location header
        if 'location' in resp.headers:
            return resp.headers["location"]
        
        # If no location header, try with redirects enabled
        resp = kwik_scraper.post(url=post_url, data=data, headers=headers, allow_redirects=True)
        
        # Check if we got a redirect to a different domain
        if resp.url != post_url and not resp.url.startswith('https://kwik.si/'):
            return resp.url
        
        # If we get a 200 response, check if it contains a direct link
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Look for download link
            download_link = soup.find('a', {'id': 'download'})
            if download_link and 'href' in download_link.attrs:
                return download_link['href']
            
            # Look for any link that ends with .mp4
            for a in soup.find_all('a', href=True):
                if a['href'].endswith('.mp4'):
                    return a['href']
            
            # Look for any script that contains a direct link
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    # Look for URLs in the script
                    urls = re.findall(r'https?://[^\s"\'<>]+', script.string)
                    for url in urls:
                        if url.endswith('.mp4'):
                            return url
        
        # If all else fails, try to extract from the final URL
        if resp.url and resp.url != link:
            return resp.url
        
        return None
    except Exception as e:
        logger.error(f"Error getting direct link: {str(e)}")
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
def extract_kwik_link(url, referer_url=None):
    """Extract kwik link from download page using cloudscraper"""
    global kwik_scraper
    if kwik_scraper is None:
        kwik_scraper = create_kwik_scraper()
    
    try:
        # Add random delay to avoid rate limiting
        time.sleep(random.uniform(1, 2))
        
        # Set headers
        local_headers = kwik_scraper.headers.copy()
        if referer_url:
            local_headers['referer'] = referer_url
        else:
            local_headers['referer'] = 'https://animepahe.ru/'
        
        # Use cloudscraper to get the page
        response = kwik_scraper.get(url, headers=local_headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        script_tags = soup.find_all('script', type="text/javascript")
        
        for script in script_tags:
            # Look for kwik.si links
            match = re.search(r'https://kwik\.si/f/[\w\d]+', script.text)
            if match:
                return match.group(0)
            
            # Also check for pahe.win links
            match = re.search(r'https://pahe\.win/[\w\d]+', script.text)
            if match:
                # Convert pahe.win link to kwik.si
                pahe_link = match.group(0)
                logger.info(f"Found pahe.win link, converting to kwik.si: {pahe_link}")
                return pahe_link.replace('pahe.win', 'kwik.si')
        
        # If we didn't find it in scripts, check for any direct links
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'kwik.si/f/' in href or 'pahe.win/' in href:
                if 'pahe.win/' in href:
                    href = href.replace('pahe.win', 'kwik.si')
                return href
        
        return None
    except Exception as e:
        logger.error(f"Error extracting kwik link: {str(e)}")
        raise

def get_episode_direct_urls(anime_title, episode_number, episode_session, quality_links, episode_url):
    """Get direct download URLs for all qualities of an episode"""
    episode_urls = {}
    for quality_link in quality_links:
        # Extract quality info
        resolution_match = re.search(r"\b(\d{3,4}p\b)", quality_link['text'])
        if not resolution_match:
            continue
        resolution = resolution_match.group(1)
        
        # Get direct download link
        try:
            kwik_link = extract_kwik_link(quality_link['href'], referer_url=episode_url)
            if not kwik_link:
                continue
            
            direct_link = get_dl_link(kwik_link)
            if direct_link:
                episode_urls[resolution] = direct_link
        except Exception as e:
            logger.error(f"Error getting direct URL for {resolution}: {str(e)}")
            continue
    
    return episode_urls

# AnimePahe API functions
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
async def search_anime(query: str) -> list:
    """Search for anime using the API"""
    search_url = f"https://animepahe.ru/api?m=search&q={quote(query)}"
    
    async with aiohttp.ClientSession() as aio_session:
        try:
            async with aio_session.get(search_url, headers=HEADERS) as response:
                response.raise_for_status()
                data = await response.json()
                
                if data.get('total', 0) == 0:
                    return []
                
                logger.info(f"Search successful for query: {query}")
                return data.get('data', [])
        except Exception as e:
            logger.error(f"Search failed for query {query}: {str(e)}")
            raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
async def get_episode_list(session_id: str, page: int = 1) -> dict:
    """Get episode list for an anime"""
    episodes_url = f"https://animepahe.ru/api?m=release&id={session_id}&sort=episode_asc&page={page}"
    
    async with aiohttp.ClientSession() as aio_session:
        try:
            async with aio_session.get(episodes_url, headers=HEADERS) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info(f"Retrieved episode list for session {session_id}, page {page}")
                return data
        except Exception as e:
            logger.error(f"Failed to get episode list for session {session_id}: {str(e)}")
            raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
async def get_episode_by_number(anime_session, episode_number, max_pages=5):
    """Get a specific episode by its number, searching through multiple pages if needed"""
    for page in range(1, max_pages + 1):
        episode_data = await get_episode_list(anime_session, page)
        if not episode_data or 'data' not in episode_data:
            return None
        
        episodes = episode_data['data']
        for ep in episodes:
            if int(ep['episode']) == episode_number:
                return ep
        
        # If we've reached the last page, break
        if page >= episode_data.get('last_page', 1):
            break
    
    return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
def get_download_links(anime_session, episode_session):
    """Get download links for an episode"""
    global scraper
    if scraper is None:
        scraper = create_scraper()
    
    if '-' in episode_session:
        episode_url = f"https://animepahe.ru/play/{episode_session}"
    else:
        episode_url = f"https://animepahe.ru/play/{anime_session}/{episode_session}"
    
    try:
        # Add random delay to avoid rate limiting
        time.sleep(random.uniform(2, 4))
        
        local_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        scraper.headers.update(local_headers)
        
        logger.info(f"Fetching episode page: {episode_url}")
        response = scraper.get(episode_url)
        response.raise_for_status()
        
        # Try html5lib first (more reliable for complex pages)
        for parser in ['html5lib', 'html.parser']:
            try:
                soup = BeautifulSoup(response.content, parser)
                break
            except:
                continue
        
        links = []
        
        selectors = [
            "#pickDownload a.dropdown-item",
            "#downloadMenu a",
            "a[download]",
            "a.btn-download",
            "a[href*='download']",
            ".download-wrapper a"
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                logger.info(f"Found {len(elements)} links with selector: {selector}")
                for element in elements:
                    href = element.get('href') or element.get('data-url') or element.get('data-href')
                    if href:
                        if not href.startswith('http'):
                            href = f"https://animepahe.ru{href}"
                        links.append({
                            'text': element.get_text(strip=True),
                            'href': href
                        })
        
        if not links:
            # Fallback search
            for a in soup.find_all('a', href=True):
                href = a['href']
                text = a.get_text(strip=True)
                if any(keyword in href.lower() or keyword in text.lower() 
                      for keyword in ['download', 'kwik.si', 'video', 'player']):
                    if not href.startswith('http'):
                        href = f"https://animepahe.ru{href}"
                    links.append({
                        'text': text or 'Download',
                        'href': href
                    })
        
        if links:
            logger.info(f"Found {len(links)} download links")
            return links
        
        logger.error(f"No download links found for episode {episode_url}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting download links: {str(e)}")
        # Recreate scraper on error
        scraper = create_scraper()
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
def get_latest_releases(page=1):
    """Get latest anime releases"""
    global scraper
    if scraper is None:
        scraper = create_scraper()
    
    releases_url = f"https://animepahe.ru/api?m=airing&page={page}"
    
    try:
        response = scraper.get(releases_url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully retrieved latest releases, page {page}")
        save_cached_releases(data.get('data', []))
        return data
    except Exception as e:
        logger.error(f"Failed to get latest releases: {str(e)}")
        # Recreate scraper on error
        if "403" in str(e) or "Forbidden" in str(e):
            logger.info("Refreshing scraper due to 403 error")
            scraper = create_scraper()
        raise

def create_initial_rss():
    """Create an initial RSS feed for fast deployment"""
    logger.info("Creating initial RSS feed for fast deployment")
    
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "AnimePahe Latest Releases"
    ET.SubElement(channel, "link").text = "https://animepahe.ru"
    ET.SubElement(channel, "description").text = "Latest anime releases from AnimePahe with direct download links (Initializing...)"
    ET.SubElement(channel, "language").text = "en-us"
    ET.SubElement(channel, "lastBuildDate").text = datetime.now(
        pytz.timezone("Asia/Kolkata")
    ).strftime("%a, %d %b %Y %H:%M:%S %z")
    
    # Add a placeholder item
    item = ET.SubElement(channel, "item")
    ET.SubElement(item, "title").text = "Initializing RSS Feed..."
    ET.SubElement(item, "description").text = "Please wait while we fetch the latest anime releases."
    ET.SubElement(item, "pubDate").text = datetime.now(
        pytz.timezone("Asia/Kolkata")
    ).strftime("%a, %d %b %Y %H:%M:%S %z")
    
    # Pretty print the XML
    rough_string = ET.tostring(rss, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="  ")
    
    # Remove extra blank lines
    pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
    
    with open(RSS_FILE, 'wb') as f:
        f.write(pretty_xml.encode('utf-8'))
    
    logger.info(f"Initial RSS feed written to {RSS_FILE}")
    return True

def create_fallback_rss():
    """Create a minimal RSS feed if generation fails"""
    logger.info("Creating fallback RSS feed")
    try:
        # Try to use cached releases
        cached_releases = load_cached_releases()
        
        rss = ET.Element("rss", version="2.0")
        channel = ET.SubElement(rss, "channel")
        ET.SubElement(channel, "title").text = "AnimePahe Latest Releases"
        ET.SubElement(channel, "link").text = "https://animepahe.ru"
        ET.SubElement(channel, "description").text = "Latest anime releases from AnimePahe (cached data)"
        ET.SubElement(channel, "language").text = "en-us"
        ET.SubElement(channel, "lastBuildDate").text = datetime.now(
            pytz.timezone("Asia/Kolkata")
        ).strftime("%a, %d %b %Y %H:%M:%S %z")
        
        # Add cached releases as basic items
        for anime in cached_releases[:MAX_ITEMS]:
            item = ET.SubElement(channel, "item")
            anime_title = anime.get('anime_title', 'Unknown Anime')
            episode_number = anime.get('episode', 0)
            ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
            ET.SubElement(item, "link").text = f"https://animepahe.ru/anime/{anime.get('anime_session', '')}"
            ET.SubElement(item, "description").text = f"Episode {episode_number} of {anime_title} (cached - download links not available)"
            ET.SubElement(item, "pubDate").text = datetime.now(
                pytz.timezone("Asia/Kolkata")
            ).strftime("%a, %d %b %Y %H:%M:%S %z")
            ET.SubElement(item, "guid").text = f"{anime_title}_Episode_{episode_number}"
        
        # Pretty print the XML
        rough_string = ET.tostring(rss, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent="  ")
        
        # Remove extra blank lines
        pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
        
        with open(RSS_FILE, 'wb') as f:
            f.write(pretty_xml.encode('utf-8'))
        
        logger.info(f"Fallback RSS feed written to {RSS_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error writing fallback RSS feed: {str(e)}")
        return False

def prettify_xml(element):
    """Return a pretty-printed XML string for the Element."""
    rough_string = ET.tostring(element, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    # Remove extra blank lines and ensure proper indentation
    pretty_xml = reparsed.toprettyxml(indent="  ")
    # Remove the XML declaration that minidom adds
    pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
    return pretty_xml

async def generate_rss_feed():
    """Generate RSS feed with the latest anime releases"""
    logger.info("Generating RSS feed with latest anime releases...")
    
    try:
        # Get latest releases
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases, using cache")
            return create_fallback_rss()
        
        new_releases = latest_data['data'][:MAX_ITEMS]
        
        if not new_releases:
            logger.error("No new releases available, creating fallback RSS feed")
            return create_fallback_rss()
        
        # Create RSS feed
        rss = ET.Element("rss", version="2.0")
        channel = ET.SubElement(rss, "channel")
        
        # Channel metadata
        ET.SubElement(channel, "title").text = "AnimePahe Latest Releases"
        ET.SubElement(channel, "link").text = "https://animepahe.ru"
        ET.SubElement(channel, "description").text = "Latest anime releases from AnimePahe with direct download links"
        ET.SubElement(channel, "language").text = "en-us"
        ET.SubElement(channel, "lastBuildDate").text = datetime.now(
            pytz.timezone("Asia/Kolkata")
        ).strftime("%a, %d %b %Y %H:%M:%S %z")
        
        # Process releases (limit to prevent rate limiting)
        processed_count = 0
        max_process = 10  # Limit processing to prevent 403 errors
        
        for anime in new_releases:
            if processed_count >= max_process:
                # Add remaining releases as basic items without download links
                anime_title = anime.get('anime_title', 'Unknown Anime')
                episode_number = anime.get('episode', 0)
                
                item = ET.SubElement(channel, "item")
                ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
                ET.SubElement(item, "link").text = f"https://animepahe.ru/anime/{anime.get('anime_session', '')}"
                ET.SubElement(item, "description").text = f"{anime_title} Episode {episode_number}"
                ET.SubElement(item, "pubDate").text = datetime.now(
                    pytz.timezone("Asia/Kolkata")
                ).strftime("%a, %d %b %Y %H:%M:%S %z")
                ET.SubElement(item, "guid").text = f"{anime_title}_Episode_{episode_number}"
                continue
            
            try:
                anime_title = anime.get('anime_title', 'Unknown Anime')
                episode_number = anime.get('episode', 0)
                release_key = f"{anime_title}_Episode_{episode_number}"
                
                logger.info(f"Processing release: {anime_title} Episode {episode_number}")
                
                # Search for the anime
                search_results = await search_anime(anime_title)
                if not search_results:
                    logger.error(f"Anime not found: {anime_title}")
                    # Add as basic item
                    item = ET.SubElement(channel, "item")
                    ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
                    ET.SubElement(item, "link").text = f"https://animepahe.ru/anime/{anime.get('anime_session', '')}"
                    ET.SubElement(item, "description").text = f"{anime_title} Episode {episode_number}"
                    ET.SubElement(item, "pubDate").text = datetime.now(
                        pytz.timezone("Asia/Kolkata")
                    ).strftime("%a, %d %b %Y %H:%M:%S %z")
                    ET.SubElement(item, "guid").text = release_key
                    continue
                
                anime_info = search_results[0]
                anime_session = anime_info['session']
                
                # Find the specific episode across multiple pages
                target_episode = await get_episode_by_number(anime_session, episode_number)
                if not target_episode:
                    logger.error(f"Episode {episode_number} not found for {anime_title} (checked multiple pages)")
                    # Add as basic item
                    item = ET.SubElement(channel, "item")
                    ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
                    ET.SubElement(item, "link").text = f"https://animepahe.ru/anime/{anime_session}"
                    ET.SubElement(item, "description").text = f"{anime_title} Episode {episode_number}"
                    ET.SubElement(item, "pubDate").text = datetime.now(
                        pytz.timezone("Asia/Kolkata")
                    ).strftime("%a, %d %b %Y %H:%M:%S %z")
                    ET.SubElement(item, "guid").text = release_key
                    continue
                
                episode_session = target_episode['session']
                
                # Try to get download links (with timeout protection)
                try:
                    download_links = get_download_links(anime_session, episode_session)
                    if not download_links:
                        logger.error(f"No download links found for {anime_title} Episode {episode_number}")
                        # Add as basic item
                        item = ET.SubElement(channel, "item")
                        ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
                        ET.SubElement(item, "link").text = f"https://animepahe.ru/anime/{anime_session}"
                        ET.SubElement(item, "description").text = f"{anime_title} Episode {episode_number}"
                        ET.SubElement(item, "pubDate").text = datetime.now(
                            pytz.timezone("Asia/Kolkata")
                        ).strftime("%a, %d %b %Y %H:%M:%S %z")
                        ET.SubElement(item, "guid").text = release_key
                        continue
                    
                    # Get direct URLs for all qualities
                    episode_urls = get_episode_direct_urls(
                        anime_title, episode_number, episode_session, download_links, 
                        f"https://animepahe.ru/play/{anime_session}/{episode_session}"
                    )
                    
                    # Create RSS item
                    item = ET.SubElement(channel, "item")
                    ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
                    
                    # Use the best available quality as primary link
                    primary_link = episode_urls.get("1080p") or episode_urls.get("720p") or episode_urls.get("360p")
                    ET.SubElement(item, "link").text = primary_link or f"https://animepahe.ru/anime/{anime_session}"
                    
                    # Create description with just the title and episode number
                    ET.SubElement(item, "description").text = f"{anime_title} Episode {episode_number}"
                    
                    # Add quality links as separate XML elements
                    if episode_urls:
                        # Sort qualities by resolution
                        for quality in sorted(episode_urls.keys(), key=lambda x: int(x[:-1])):
                            url = episode_urls[quality]
                            quality_element = ET.SubElement(item, quality)
                            quality_element.text = url
                    
                    ET.SubElement(item, "pubDate").text = datetime.now(
                        pytz.timezone("Asia/Kolkata")
                    ).strftime("%a, %d %b %Y %H:%M:%S %z")
                    ET.SubElement(item, "author").text = "Blakite_Ravii"
                    ET.SubElement(item, "guid").text = primary_link or release_key
                    
                    processed_count += 1
                    logger.info(f"Added {anime_title} Episode {episode_number} with {len(episode_urls)} quality links")
                    
                    # Add delay between processing to avoid rate limits
                    await asyncio.sleep(random.uniform(1, 2))
                
                except Exception as e:
                    logger.error(f"Error processing download links for {anime_title} Episode {episode_number}: {str(e)}")
                    # Add as basic item
                    item = ET.SubElement(channel, "item")
                    ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
                    ET.SubElement(item, "link").text = f"https://animepahe.ru/anime/{anime.get('anime_session', '')}"
                    ET.SubElement(item, "description").text = f"{anime_title} Episode {episode_number}"
                    ET.SubElement(item, "pubDate").text = datetime.now(
                        pytz.timezone("Asia/Kolkata")
                    ).strftime("%a, %d %b %Y %H:%M:%S %z")
                    ET.SubElement(item, "guid").text = release_key
                
            except Exception as e:
                logger.error(f"Error processing {anime_title} Episode {episode_number}: {str(e)}")
                continue
        
        # Pretty print the XML
        pretty_xml = prettify_xml(rss)
        
        with open(RSS_FILE, 'wb') as f:
            f.write(pretty_xml.encode('utf-8'))
        
        logger.info(f"RSS feed written to {RSS_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error generating RSS feed: {str(e)}")
        return create_fallback_rss()

async def update_rss_loop():
    """Background task to update RSS feed periodically"""
    while True:
        try:
            logger.info("Starting RSS feed update...")
            await generate_rss_feed()
            logger.info("RSS feed update completed")
        except Exception as e:
            logger.error(f"Error in RSS update loop: {str(e)}")
            create_fallback_rss()
        
        # Wait before next update
        sleep_time = random.uniform(UPDATE_INTERVAL_MIN, UPDATE_INTERVAL_MAX)
        logger.info(f"Next update in {sleep_time:.1f} seconds")
        await asyncio.sleep(sleep_time)

@app.route('/')
def serve_rss():
    """Serve the RSS feed file"""
    try:
        if RSS_FILE.exists():
            logger.info(f"Serving RSS feed from {RSS_FILE}")
            return send_file(RSS_FILE, mimetype='application/rss+xml')
        else:
            logger.error(f"RSS feed file not found at {RSS_FILE}")
            create_initial_rss()
            if RSS_FILE.exists():
                logger.info(f"Serving initial RSS feed from {RSS_FILE}")
                return send_file(RSS_FILE, mimetype='application/rss+xml')
            return "RSS feed not found", 404
    except Exception as e:
        logger.error(f"Error serving RSS feed: {str(e)}")
        return "Error serving RSS feed", 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    if RSS_FILE.exists():
        return Response("Service is running and RSS feed is available", status=200)
    else:
        return Response("Service is running but RSS feed is not available", status=503)

@app.route('/status')
def status():
    """Status endpoint with details"""
    status_info = {
        'rss_file_exists': RSS_FILE.exists(),
        'cache_file_exists': CACHE_FILE.exists(),
        'last_update': datetime.now().isoformat(),
        'max_items': MAX_ITEMS,
        'update_interval': f"{UPDATE_INTERVAL_MIN}-{UPDATE_INTERVAL_MAX}s"
    }
    
    if RSS_FILE.exists():
        status_info['rss_file_size'] = RSS_FILE.stat().st_size
        status_info['rss_file_modified'] = datetime.fromtimestamp(RSS_FILE.stat().st_mtime).isoformat()
    
    return status_info

def start_flask():
    """Start Flask server in a separate thread"""
    port = int(os.environ.get('PORT', 8000))
    logger.info(f"Starting Flask server on port {port}")
    app.run(host='0.0.0.0', port=port)

def main():
    """Main function"""
    logger.info("Starting AnimePahe RSS Feed Generator...")
    
    # Create initial RSS feed for fast deployment
    create_initial_rss()
    
    # Initialize session
    global scraper, session, kwik_scraper
    scraper = create_scraper()
    session = create_session()
    kwik_scraper = create_kwik_scraper()
    
    # Start Flask server in a separate thread
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask server started")
    
    # Give Flask server time to start
    time.sleep(1)
    
    # Start RSS update loop
    logger.info("Starting RSS update loop...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(update_rss_loop())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error in main loop: {str(e)}")

if __name__ == '__main__':
    main()
