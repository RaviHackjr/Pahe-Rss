```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AnimePahe RSS Feed Generator with Multi-Quality Support
Generates an RSS feed for the latest 25 anime releases from AnimePahe with 360p, 720p, and 1080p links
Designed for Koyeb deployment with auto-updates every 10-30 seconds, maintaining 25 items with newest at top
"""
import logging
import os
import re
import time
import random
import asyncio
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

# Headers for requests
HEADERS = {
    'authority': 'animepahe.ru',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
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
QUALITY_PREFERENCES = ["360p", "720p", "1080p"]
UPDATE_INTERVAL_MIN = 10  # seconds
UPDATE_INTERVAL_MAX = 30  # seconds
MAX_ITEMS = 25  # Maximum number of items in RSS feed

# Create directories
BASE_DIR.mkdir(parents=True, exist_ok=True)

# Store previous releases to detect new episodes
previous_releases = set()

# Create cloudscraper instance
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
    interpreter='nodejs'
)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
async def search_anime(query: str) -> list:
    search_url = f"https://animepahe.ru/api?m=search&q={quote(query)}"
    async with aiohttp.ClientSession() as session:
        async with session.get(search_url, headers=HEADERS) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get('data', [])

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
async def get_episode_list(session_id: str, page: int = 1) -> dict:
    episodes_url = f"https://animepahe.ru/api?m=release&id={session_id}&sort=episode_asc&page={page}"
    async with aiohttp.ClientSession() as session:
        async with session.get(episodes_url, headers=HEADERS) as response:
            response.raise_for_status()
            return await response.json()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
def get_download_links(anime_session, episode_session):
    if '-' in episode_session:
        episode_url = f"https://animepahe.ru/play/{episode_session}"
    else:
        episode_url = f"https://animepahe.ru/play/{anime_session}/{episode_session}"
    
    session = requests.Session()
    session.headers.update(HEADERS)
    time.sleep(random.uniform(2, 5))
    local_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }
    session.headers.update(local_headers)
    session.get("https://animepahe.ru/")
    logger.info(f"Fetching episode page: {episode_url}")
    response = session.get(episode_url)
    response.raise_for_status()
    
    for parser in ['lxml', 'html.parser', 'html5lib']:
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
    
    if links:
        logger.info(f"Found {len(links)} download links")
        return links
    logger.error(f"No download links found for episode {episode_url}")
    return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def extract_kwik_link(url):
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
    
    scraper.get("https://animepahe.ru/", headers=headers)
    response = scraper.get(url, headers=headers)
    response.raise_for_status()
    
    for parser in ['lxml', 'html.parser', 'html5lib']:
        try:
            soup = BeautifulSoup(response.text, parser)
            break
        except Exception as e:
            logger.warning(f"Parser {parser} failed: {str(e)}")
            continue
    
    for script in soup.find_all('script'):
        if script.string:
            match = re.search(r'https://kwik\.si/f/[\w\d-]+', script.string)
            if match:
                return match.group(0)
    
    download_elements = soup.select('a[href*="kwik.si"], a[onclick*="kwik.si"]')
    for element in download_elements:
        href = element.get('href') or element.get('onclick', '')
        match = re.search(r'https://kwik\.si/f/[\w\d-]+', href)
        if match:
            return match.group(0)
    
    page_text = str(soup)
    matches = re.findall(r'https://kwik\.si/f/[\w\d-]+', page_text)
    if matches:
        return matches[0]
    
    logger.error(f"No kwik link found for {url}")
    return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def get_latest_releases(page=1):
    releases_url = f"https://animepahe.ru/api?m=airing&page={page}"
    try:
        response = scraper.get(releases_url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to get latest releases: {str(e)}")
        raise

def create_fallback_rss():
    """Create a minimal RSS feed if generation fails"""
    logger.info("Creating fallback RSS feed")
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "AnimePahe Latest Releases"
    ET.SubElement(channel, "link").text = "https://animepahe.ru"
    ET.SubElement(channel, "description").text = "Latest anime releases from AnimePahe with 360p, 720p, and 1080p download links (temporarily unavailable)"
    ET.SubElement(channel, "language").text = "en-us"
    ET.SubElement(channel, "lastBuildDate").text = datetime.now(
        pytz.timezone("Asia/Kolkata")
    ).strftime("%a, %d %b %Y %H:%M:%S %z")
    try:
        tree = ET.ElementTree(rss)
        with open(RSS_FILE, 'wb') as f:
            tree.write(f, encoding='utf-8', xml_declaration=True)
        logger.info(f"Fallback RSS feed written to {RSS_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error writing fallback RSS feed: {str(e)}")
        return False

async def generate_rss_feed(new_releases=None):
    """Generate or update RSS feed with the latest anime releases, maintaining 25 items"""
    global previous_releases
    logger.info("Checking for new anime releases to update RSS feed...")
    
    try:
        # Load existing RSS feed if it exists
        items = []
        if RSS_FILE.exists():
            try:
                tree = ET.parse(RSS_FILE)
                root = tree.getroot()
                channel = root.find("channel")
                items = channel.findall("item")
                logger.info(f"Loaded {len(items)} existing items from RSS feed")
            except Exception as e:
                logger.warning(f"Error reading existing RSS feed: {str(e)}. Starting fresh.")
                items = []
        
        # Get latest releases if not provided (for initial run or full refresh)
        if new_releases is None:
            latest_data = get_latest_releases(page=1)
            if not latest_data or 'data' not in latest_data:
                logger.error("Failed to get latest releases, creating fallback RSS feed")
                return create_fallback_rss()
            new_releases = latest_data['data'][:MAX_ITEMS]
        
        # Create or update RSS feed
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
        
        # Process new releases
        new_items = []
        for anime in new_releases:
            try:
                anime_title = anime.get('anime_title', 'Unknown Anime')
                episode_number = anime.get('episode', 0)
                release_key = f"{anime_title}_Episode_{episode_number}"
                
                if release_key in previous_releases:
                    continue  # Skip already processed episodes
                
                logger.info(f"Processing new release: {anime_title} Episode {episode_number}")
                
                # Search for the anime
                search_results = await search_anime(anime_title)
                if not search_results:
                    logger.error(f"Anime not found: {anime_title}")
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
                
                # Get download links
                download_links = get_download_links(anime_session, episode_session)
                if not download_links:
                    logger.error(f"No download links found for {anime_title} Episode {episode_number}")
                    continue
                
                # Find links for each quality
                quality_links = {}
                for quality in QUALITY_PREFERENCES:
                    for link in download_links:
                        if quality in link['text']:
                            kwik_link = extract_kwik_link(link['href'])
                            if kwik_link:
                                quality_links[quality] = kwik_link
                
                if not quality_links:
                    logger.error(f"No quality links found for {anime_title} Episode {episode_number}")
                    continue
                
                # Create RSS item
                item = ET.Element("item")
                ET.SubElement(item, "title").text = f"{anime_title} Episode {episode_number}"
                primary_link = quality_links.get("1080p") or quality_links.get("720p") or quality_links.get("360p")
                ET.SubElement(item, "link").text = primary_link
                description = f"Episode {episode_number} of {anime_title}\n\n"
                for quality, link in quality_links.items():
                    description += f"{quality}: {link}\n"
                ET.SubElement(item, "description").text = description
                ET.SubElement(item, "pubDate").text = datetime.now(
                    pytz.timezone("Asia/Kolkata")
                ).strftime("%a, %d %b %Y %H:%M:%S %z")
                ET.SubElement(item, "author").text = "Blakite_Ravii"
                ET.SubElement(item, "guid").text = primary_link
                
                new_items.append((item, release_key))
                logger.info(f"Added {anime_title} Episode {episode_number} with {len(quality_links)} quality links")
            
            except Exception as e:
                logger.error(f"Error processing {anime_title} Episode {episode_number}: {str(e)}")
                continue
        
        # Combine new and existing items (newest at top)
        all_items = new_items + [(item, f"{item.find('title').text}") for item in items]
        all_items = all_items[:MAX_ITEMS]  # Keep only the latest 25 items
        
        # Add items to channel
        for item, _ in all_items:
            channel.append(item)
        
        # Update previous_releases
        previous_releases = set(key for _, key in all_items)
        
        # Write RSS feed to file
        try:
            with open(RSS_FILE, 'wb') as f:
                tree = ET.ElementTree(rss)
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
    """Background task to update RSS feed every 10-30 seconds with new releases"""
    global previous_releases
    while True:
        try:
            # Get latest releases
            latest_data = get_latest_releases(page=1)
            if not latest_data or 'data' not in latest_data:
                logger.error("Failed to get latest releases")
                create_fallback_rss()
                await asyncio.sleep(random.uniform(UPDATE_INTERVAL_MIN, UPDATE_INTERVAL_MAX))
                continue
            
            # Check for new releases
            new_releases = []
            for anime in latest_data['data'][:MAX_ITEMS]:
                release_key = f"{anime.get('anime_title', 'Unknown Anime')}_Episode_{anime.get('episode', 0)}"
                if release_key not in previous_releases:
                    new_releases.append(anime)
            
            if new_releases:
                logger.info(f"Found {len(new_releases)} new releases")
                await generate_rss_feed(new_releases=new_releases)
            else:
                logger.info("No new releases found")
        
        except Exception as e:
            logger.error(f"Error in RSS update loop: {str(e)}")
            create_fallback_rss()
        
        # Random delay between 10-30 seconds
        await asyncio.sleep(random.uniform(UPDATE_INTERVAL_MIN, UPDATE_INTERVAL_MAX))

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
    """Health check endpoint to verify service status"""
    if RSS_FILE.exists():
        return Response("Service is running and RSS feed is available", status=200)
    else:
        return Response("Service is running but RSS feed is not available", status=503)

def start_flask():
    """Start Flask server in a separate thread"""
    port = int(os.environ.get('PORT', 8000))
    logger.info(f"Starting Flask server on port {port}")
    app.run(host='0.0.0.0', port=port)

def main():
    """Synchronous main function to ensure initial RSS feed generation"""
    logger.info("Starting RSS feed generator service...")
    
    # Generate initial RSS feed synchronously
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
    loop.run_until_complete(update_rss_loop())

if __name__ == '__main__':
    main()
