import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict
import asyncio
from playwright.async_api import async_playwright

class MovieScraper:
    """Scrape movie listings from various NYC sources"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def generate_letterboxd_url(self, title: str) -> str:
        """Generate Letterboxd URL from movie title"""
        import re
        
        # Convert title to lowercase and handle year format
        # "Frankenstein (2025)" -> "frankenstein-2025"
        
        # Extract year if in parentheses
        year_match = re.search(r'\((\d{4})\)', title)
        if year_match:
            year = year_match.group(1)
            # Remove year and parentheses from title
            clean_title = re.sub(r'\s*\(\d{4}\)', '', title)
        else:
            year = ''
            clean_title = title
        
        # Convert to lowercase and replace spaces/special chars with hyphens
        slug = re.sub(r'[^a-zA-Z0-9]+', '-', clean_title.lower())
        # Remove leading/trailing hyphens
        slug = slug.strip('-')
        
        # Add year if present
        if year:
            slug = f"{slug}-{year}"
        
        return f"https://letterboxd.com/film/{slug}/"
    
    async def scrape_alamo_drafthouse_async(self) -> List[Dict]:
        """Scrape Alamo Drafthouse NYC using Playwright"""
        movies = []
        print('Scraping Alamo Drafthouse with Playwright...')
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto('https://drafthouse.com/nyc', wait_until='networkidle')
                
                # Wait for movie content to load
                await page.wait_for_timeout(3000)
                
                # Click "Load more" button if it exists
                try:
                    load_more_button = page.locator('ion-button:has-text("Load more"), button:has-text("Load more"), [aria-label*="load more" i]')
                    while await load_more_button.count() > 0:
                        print("Found 'Load more' button, clicking...")
                        await load_more_button.first.click()
                        await page.wait_for_timeout(2000)  # Wait for new content to load
                except Exception as e:
                    print(f"No 'Load more' button found or error clicking: {e}")
                
                # Get the rendered HTML content
                # html_content = await page.content()
                # print("=== RENDERED HTML CONTENT ===")
                # print(html_content)
                # print("=== END HTML CONTENT ===")
                
                # Get all movie elements using the ion-card-title structure
                movie_data = await page.evaluate('''
                    () => {
                        const movieTitles = document.querySelectorAll('ion-card-title div');
                        return Array.from(movieTitles).map(el => {
                            const title = el.textContent?.trim();
                            const card = el.closest('ion-card');
                            const link = card?.querySelector('a')?.href || '';
                            return {
                                title: title || 'Unknown',
                                url: link,
                                text: el.textContent.trim()
                            };
                        });
                    }
                ''')
                
                await browser.close()
                
                for item in movie_data:
                    if item['title'] and item['title'] != 'Unknown':
                        movies.append({
                            'title': item['title'],
                            'venue': 'Alamo Drafthouse',
                            'url': item['url'],
                            'source': 'alamo',
                            'letterboxd_url': self.generate_letterboxd_url(item['title'])
                        })
                
                print(f"Found {len(movies)} movies at Alamo Drafthouse")
                
        except Exception as e:
            print(f"Error scraping Alamo with Playwright: {e}")
        
        return movies
    
    def scrape_alamo_drafthouse(self) -> List[Dict]:
        """Scrape Alamo Drafthouse NYC - wrapper for async method"""
        return asyncio.run(self.scrape_alamo_drafthouse_async())
    
    def scrape_metrograph(self) -> List[Dict]:
        """Scrape Metrograph"""
        movies = []
        url = "https://metrograph.com/film/"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'lxml')
            
            for item in soup.select('.film-item'):  # Adjust selector
                title = item.select_one('.film-title')
                if title:
                    movies.append({
                        'title': title.text.strip(),
                        'venue': 'Metrograph',
                        'url': item.get('href', ''),
                        'source': 'metrograph'
                    })
        except Exception as e:
            print(f"Error scraping Metrograph: {e}")
        
        return movies
    
    def scrape_ifc_center(self) -> List[Dict]:
        """Scrape IFC Center"""
        movies = []
        url = "https://www.ifccenter.com/"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'lxml')
            
            for item in soup.select('.movie'):
                title = item.select_one('h3')
                if title:
                    movies.append({
                        'title': title.text.strip(),
                        'venue': 'IFC Center',
                        'url': item.get('href', ''),
                        'source': 'ifc'
                    })
        except Exception as e:
            print(f"Error scraping IFC: {e}")
        
        return movies
    
    def get_all_movies(self) -> List[Dict]:
        """Aggregate movies from all sources"""
        all_movies = []
        all_movies.extend(self.scrape_alamo_drafthouse())
        all_movies.extend(self.scrape_metrograph())
        all_movies.extend(self.scrape_ifc_center())
        
        # Deduplicate by title (case-insensitive)
        seen = set()
        unique_movies = []
        for movie in all_movies:
            title_lower = movie['title'].lower()
            if title_lower not in seen:
                seen.add(title_lower)
                unique_movies.append(movie)
        
        return unique_movies
