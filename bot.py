#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AnimePahe RSS Feed Generator with Multi-Quality Support
Generates an RSS feed for the latest 25 anime releases from AnimePahe with 360p, 720p, and 1080p links
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
import cloudscraper
from flask import Flask, send_file, Response

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

# Headers for requests (from working bot)
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
UPDATE_INTERVAL_MIN = 30  # seconds (increased to avoid rate limits)
UPDATE_INTERVAL_MAX = 60  # seconds
MAX_ITEMS = 25  # Maximum number of items in RSS feed

# Create directories
BASE_DIR.mkdir(parents=True, exist_ok=True)

# Store previous releases to detect new episodes
previous_releases = set()

# Global session with proper cookie handling
session = None
scraper = None

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
        time.sleep(random.uniform(2, 4))  # Respectful delay
    except Exception as e:
        logger.error(f"Failed to initialize cloudscraper session: {str(e)}")
    
    return scraper

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
        time.sleep(random.uniform(2, 4))  # Respectful delay
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

def step_2(s, seperator, base=10):
    """Helper function for kwik link extraction"""
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
    """Helper function for kwik link extraction"""
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
        time.sleep(random.uniform(3, 6))
        
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
def extract_kwik_link(url):
    """Extract kwik.si link from download page"""
    global scraper
    if scraper is None:
        scraper = create_scraper()
    
    try:
        # Add random delay
        time.sleep(random.uniform(2, 4))
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://animepahe.ru/'
        }
        
        response = scraper.get(url, headers=headers)
        response.raise_for_status()
        
        logger.info(f"Got response from {url}, status code: {response.status_code}")
        
        # Try html5lib first (more reliable for complex pages)
        for parser in ['html5lib', 'html.parser']:
            try:
                soup = BeautifulSoup(response.text, parser)
                break
            except Exception as e:
                logger.warning(f"Parser {parser} failed: {str(e)}")
                continue
        
        # Look for the kwik link in script tags
        for script in soup.find_all('script'):
            if script.string:
                match = re.search(r'https://kwik\.si/f/[\w\d-]+', script.string)
                if match:
                    return match.group(0)
        
        # Look for download elements
        download_elements = soup.select('a[href*="kwik.si"], a[onclick*="kwik.si"]')
        for element in download_elements:
            href = element.get('href') or element.get('onclick', '')
            match = re.search(r'https://kwik\.si/f/[\w\d-]+', href)
            if match:
                return match.group(0)
        
        # Look in the whole page text
        page_text = str(soup)
        matches = re.findall(r'https://kwik\.si/f/[\w\d-]+', page_text)
        if matches:
            return matches[0]
        
        return None
    except Exception as e:
        logger.error(f"Error extracting kwik link: {str(e)}")
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
def get_dl_link(link):
    """Get direct download link from kwik.si"""
    global scraper
    if scraper is None:
        scraper = create_scraper()
    
    try:
        # Add random delay to avoid rate limiting
        time.sleep(random.uniform(2, 4))
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
        
        # First, get the main page to establish session
        scraper.get("https://animepahe.ru/", headers=headers)
        
        # Now get the actual link
        resp = scraper.get(link, headers=headers)
        
        # Try different patterns to extract the parameters
        patterns = [
            r'\("([^"]+)",(\d+),"([^"]+)",(\d+),(\d+)',
            r'\("(\S+)",\d+,"(\S+)",(\d+),(\d+)'
        ]
        
        match = None
        for pattern in patterns:
            match = re.search(pattern, resp.text)
            if match:
                break
        
        if not match:
            logger.error(f"Could not find required pattern in response from {link}")
            return None
        
        # Extract parameters based on the pattern matched
        if len(match.groups()) == 5:
            data, _, key, load, seperator = match.groups()
        else:
            data, key, load, seperator = match.groups()
        
        # Process the parameters
        url, token = step_1(data=data, key=key, load=load, seperator=seperator)
        
        # Prepare the POST request
        post_url = url if url.startswith('http') else f"https://kwik.si{url}"
        data = {"_token": token}
        post_headers = {
            'referer': link,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://kwik.si'
        }
        
        # Make the POST request
        resp = scraper.post(url=post_url, data=data, headers=post_headers, allow_redirects=False)
        
        # Check for redirect in headers
        if 'location' in resp.headers:
            return resp.headers["location"]
        
        # If no redirect, follow redirects manually
        resp = scraper.post(url=post_url, data=data, headers=post_headers, allow_redirects=True)
        
        # Check if the final URL is different from the post URL
        if resp.url != post_url and not resp.url.startswith('https://kwik.si/'):
            return resp.url
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting direct link: {str(e)}")
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
        
        tree = ET.ElementTree(rss)
        with open(RSS_FILE, 'wb') as f:
            tree.write(f, encoding='utf-8', xml_declaration=True)
        logger.info(f"Fallback RSS feed written to {RSS_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error writing fallback RSS feed: {str(e)}")
        return False

async def generate_rss_feed():
    """Generate RSS feed with the latest anime releases"""
    global previous_releases
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
        ET.SubElement(channel, "description").text = "Latest anime releases from AnimePahe with 360p, 720p, and 1080p download links"
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
                ET.SubElement(item, "description").text = f"Episode {episode_number} of {anime_title} - Visit AnimePahe for download links"
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
                    ET.SubElement(item, "description").text = f"Episode {episode_number} of {anime_title} - Visit AnimePahe for download links"
                    ET.SubElement(item, "pubDate").text = datetime.now(
                        pytz.timezone("Asia/Kolkata")
                    ).strftime("%a, %d %b %Y %H:%M:%S %z")
                    ET.SubElement(item, "guid").text = release_key
                    continue
                
                anime_info = search_results[0]
                anime_session = anime_info['session']
                
                # Get episode list
                episode_data = await get_episode_list(anime_session)
                if not episode_data or 'data' not in episode_data:
                    logger.error(f"Failed to get episode list for {anime_title}")
                    continue
                
                # Find the specific episode
                episodes = episode_data['data']
                target_episode = None
                for ep in episodes:
                    if int(ep['episode']) == episode_number:
                        target_episode = ep
                        break
                
                if not target_episode:
                    logger.error(f"Episode {episode_number} not found for {anime_title}")
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
                        ET.SubElement(item, "description").text = f"Episode {episode_number} of {anime_title} - Visit AnimePahe for download links"
                        ET.SubElement(item, "pubDate").text = datetime.now(
                            pytz.timezone("Asia/Kolkata")
                        ).strftime("%a, %d %b %Y %H:%M:%S %z")
                        ET.SubElement(item, "guid").text = release_key
                        continue
                    
                    # Find links for each quality
                    quality_links = {}
                    for quality in QUALITY_PREFERENCES:
                        for link in download_links:
                            if quality in link['text']:
                                try:
                                    kwik_link = extract_kwik_link(link['href'])
                                    if kwik_link:
                                        # Try to get direct link
                                        direct_link = get_dl_link(kwik_link)
                                        quality_links[quality] = direct_link or kwik_link
                                        break
                                except Exception as e:
                                    logger.error(f"Error getting {quality} link: {str(e)}")
                                    # Use kwik link as fallback
                                    kwik_link = extract_kwik_link(link['href'])
                                    if kwik_link:
                                        quality_links[quality] = kwik_link
                                        break
                    
                    # Create RSS item
                    item = ET.SubElement(channel, "item")
                    ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
                    
                    # Use the best available quality as primary link
                    primary_link = quality_links.get("1080p") or quality_links.get("720p") or quality_links.get("360p")
                    ET.SubElement(item, "link").text = primary_link or f"https://animepahe.ru/anime/{anime_session}"
                    
                    # Create description with all quality links
                    description = f"Episode {episode_number} of {anime_title}\n\n"
                    if quality_links:
                        description += "Download Links:\n"
                        for quality, link in quality_links.items():
                            description += f"{quality}: {link}\n"
                    else:
                        description += "Visit AnimePahe for download links"
                    
                    ET.SubElement(item, "description").text = description
                    ET.SubElement(item, "pubDate").text = datetime.now(
                        pytz.timezone("Asia/Kolkata")
                    ).strftime("%a, %d %b %Y %H:%M:%S %z")
                    ET.SubElement(item, "author").text = "AnimePahe"
                    ET.SubElement(item, "guid").text = primary_link or release_key
                    
                    processed_count += 1
                    logger.info(f"Added {anime_title} Episode {episode_number} with {len(quality_links)} quality links")
                    
                    # Add delay between processing to avoid rate limits
                    await asyncio.sleep(random.uniform(2, 4))
                
                except Exception as e:
                    logger.error(f"Error processing download links for {anime_title} Episode {episode_number}: {str(e)}")
                    # Add as basic item
                    item = ET.SubElement(channel, "item")
                    ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
                    ET.SubElement(item, "link").text = f"https://animepahe.ru/anime/{anime.get('anime_session', '')}"
                    ET.SubElement(item, "description").text = f"Episode {episode_number} of {anime_title} - Visit AnimePahe for download links"
                    ET.SubElement(item, "pubDate").text = datetime.now(
                        pytz.timezone("Asia/Kolkata")
                    ).strftime("%a, %d %b %Y %H:%M:%S %z")
                    ET.SubElement(item, "guid").text = release_key
                
            except Exception as e:
                logger.error(f"Error processing {anime_title} Episode {episode_number}: {str(e)}")
                continue
        
        # Write RSS feed to file
        try:
            tree = ET.ElementTree(rss)
            with open(RSS_FILE, 'wb') as f:
                tree.write(f, encoding='utf-8', xml_declaration=True)
            logger.info(f"RSS feed written to {RSS_FILE}")
            return True
        except Exception as e:
            logger.error(f"Error writing RSS feed to file: {str(e)}")
            return create_fallback_rss()
    
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
            create_fallback_rss()
            if RSS_FILE.exists():
                logger.info(f"Serving fallback RSS feed from {RSS_FILE}")
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
    
    # Initialize session
    global scraper, session
    scraper = create_scraper()
    session = create_session()
    
    # Generate initial RSS feed
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(generate_rss_feed())
    except Exception as e:
        logger.error(f"Initial RSS feed generation failed: {str(e)}")
        create_fallback_rss()
    finally:
        loop.close()
    
    # Start Flask server in a separate thread
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()
    
    # Start RSS update loop
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
