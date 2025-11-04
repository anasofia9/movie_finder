import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict
import asyncio
import json
import os
import pytz
from playwright.async_api import async_playwright

class MovieScraper:
    """Scrape movie listings from various NYC sources"""
    
    def __init__(self, log_callback=None, use_cache=True):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.log = log_callback or print
        self.cache_file = 'theater_cache.json'
        self.theater_cache = {}
        self.eastern_tz = pytz.timezone('US/Eastern')
        self.use_cache = use_cache
        if self.use_cache:
            self._load_theater_cache()
    
    def _load_theater_cache(self):
        """Load theater cache from JSON file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.theater_cache = json.load(f)
                self.log(f"📂 Loaded theater cache with {len(self.theater_cache)} entries")
            except Exception as e:
                self.log(f"⚠️  Error loading theater cache: {e}")
                self.theater_cache = {}
    
    def _save_theater_cache(self):
        """Save theater cache to JSON file"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.theater_cache, f, indent=2, default=str)
        except Exception as e:
            self.log(f"⚠️  Error saving theater cache: {e}")
    
    def _get_eastern_date_string(self):
        """Get current date string in Eastern timezone (YYYY-MM-DD)"""
        eastern_now = datetime.now(self.eastern_tz)
        return eastern_now.strftime('%Y-%m-%d')
    
    def _is_cache_valid(self, theater_id: str) -> bool:
        """Check if cached data for theater is still valid (same day in Eastern time)"""
        if theater_id not in self.theater_cache:
            return False
        
        cached_date = self.theater_cache[theater_id].get('date')
        current_date = self._get_eastern_date_string()
        
        return cached_date == current_date
    
    def _get_cached_movies(self, theater_id: str) -> List[Dict]:
        """Get cached movies for a theater"""
        if self._is_cache_valid(theater_id):
            return self.theater_cache[theater_id].get('movies', [])
        return []
    
    def _cache_movies(self, theater_id: str, movies: List[Dict]):
        """Cache movies for a theater with current date"""
        self.theater_cache[theater_id] = {
            'date': self._get_eastern_date_string(),
            'movies': movies,
            'cached_at': datetime.now(self.eastern_tz).isoformat()
        }
        self._save_theater_cache()
    
    def get_cache_status(self) -> Dict:
        """Get cache status for all theaters"""
        theater_names = {
            'alamo': 'Alamo Drafthouse',
            'metrograph': 'Metrograph', 
            'ifc': 'IFC Center',
            'angelika': 'Angelika Film Center',
            'angelika_village_east': 'Angelika Village East',
            'paris_theater': 'Paris Theater',
            'nitehawk_williamsburg': 'Nitehawk Williamsburg',
            'nitehawk_prospect_park': 'Nitehawk Prospect Park',
            'moving_image': 'Museum of Moving Image',
            'film_forum': 'Film Forum'
        }
        
        current_date = self._get_eastern_date_string()
        cache_info = {}
        
        for theater_id, theater_name in theater_names.items():
            is_cached = self._is_cache_valid(theater_id)
            movie_count = len(self._get_cached_movies(theater_id)) if is_cached else 0
            cache_info[theater_id] = {
                'name': theater_name,
                'cached': is_cached,
                'movie_count': movie_count,
                'date': current_date
            }
            
        return cache_info
    
    def generate_letterboxd_url(self, title: str) -> str:
        """Generate Letterboxd URL from movie title"""
        import re
        
        
        # Convert title to lowercase and handle year format
        # "Frankenstein (2025)" -> "frankenstein-2025"
        
        # Remove "Presents:" prefixes like "ACE Presents: A Nightmare on Elm Street"
        clean_title = re.sub(r'^.*?\s+Presents:\s*', '', title, flags=re.IGNORECASE)
        
        # Remove common suffixes
        clean_title = re.sub(r'\s*\(Subtitled\)$', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s*\(Dubbed\)$', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s*Remastered$', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s*Movie Party$', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r':?\s*\d+(?:st|nd|rd|th)\s*Anniversary$', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s*Early Access$', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s*with Live Q&A$', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s*Re-?release$', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s*A Sing-Along Event$', '', clean_title, flags=re.IGNORECASE)
        # Remove reconstruction/restoration suffixes like "(1998 Reconstruction)"
        clean_title = re.sub(r'\s*\(\d{4}\s+Reconstruction\)$', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s*\(\d{4}\s+Restoration\)$', '', clean_title, flags=re.IGNORECASE)
        # Remove format/technical suffixes like "[35mm]", "[4K]", "[Digital]"
        clean_title = re.sub(r'\s*\[[^\]]+\]$', '', clean_title, flags=re.IGNORECASE)
        # Remove "in XXmm" suffixes like "in 35MM", "in 16mm", "in 70MM"
        clean_title = re.sub(r'\s*in\s+\d+mm$', '', clean_title, flags=re.IGNORECASE)
        # Remove director's cut suffixes
        clean_title = re.sub(r'\s*:\s*The Director\'?s Cut$', '', clean_title, flags=re.IGNORECASE)
        
        # Extract year if in parentheses
        year_match = re.search(r'\((\d{4})\)', clean_title)
        if year_match:
            year = year_match.group(1)
            # Remove year and parentheses from title
            clean_title = re.sub(r'\s*\(\d{4}\)', '', clean_title)
        else:
            year = ''
        
        
        # Handle contractions - chain multiple substitutions since character class isn't working reliably
        clean_title = re.sub(r"\u0027s\b", "s", clean_title)     # 's -> s (straight apostrophe)
        clean_title = re.sub(r"\u2019s\b", "s", clean_title)     # 's -> s (right single quotation)
        clean_title = re.sub(r"\u2018s\b", "s", clean_title)     # 's -> s (left single quotation)
        clean_title = re.sub(r"\u0060s\b", "s", clean_title)     # `s -> s (grave accent)
        
        clean_title = re.sub(r"\u0027d\b", "d", clean_title)     # 'd -> d
        clean_title = re.sub(r"\u2019d\b", "d", clean_title)     # 'd -> d
        clean_title = re.sub(r"\u2018d\b", "d", clean_title)     # 'd -> d
        clean_title = re.sub(r"\u0060d\b", "d", clean_title)     # `d -> d
        
        clean_title = re.sub(r"\u0027t\b", "t", clean_title)     # 't -> t
        clean_title = re.sub(r"\u2019t\b", "t", clean_title)     # 't -> t
        clean_title = re.sub(r"\u2018t\b", "t", clean_title)     # 't -> t
        clean_title = re.sub(r"\u0060t\b", "t", clean_title)     # `t -> t
        
        clean_title = re.sub(r"\u0027ll\b", "ll", clean_title)   # 'll -> ll
        clean_title = re.sub(r"\u2019ll\b", "ll", clean_title)   # 'll -> ll
        clean_title = re.sub(r"\u2018ll\b", "ll", clean_title)   # 'll -> ll
        clean_title = re.sub(r"\u0060ll\b", "ll", clean_title)   # `ll -> ll
        
        clean_title = re.sub(r"\u0027re\b", "re", clean_title)   # 're -> re
        clean_title = re.sub(r"\u2019re\b", "re", clean_title)   # 're -> re
        clean_title = re.sub(r"\u2018re\b", "re", clean_title)   # 're -> re
        clean_title = re.sub(r"\u0060re\b", "re", clean_title)   # `re -> re
        
        clean_title = re.sub(r"\u0027ve\b", "ve", clean_title)   # 've -> ve
        clean_title = re.sub(r"\u2019ve\b", "ve", clean_title)   # 've -> ve
        clean_title = re.sub(r"\u2018ve\b", "ve", clean_title)   # 've -> ve
        clean_title = re.sub(r"\u0060ve\b", "ve", clean_title)   # `ve -> ve
        
        
        # Remove any remaining apostrophes, periods, and replace ampersands
        clean_title = clean_title.replace("'", "").replace("'", "").replace("`", "").replace("'", "").replace("'", "").replace("ʼ", "").replace("ˈ", "").replace(".", "").replace("&", "and")
        # Convert accented characters to unaccented equivalents
        import unicodedata
        clean_title = unicodedata.normalize('NFD', clean_title)
        clean_title = ''.join(c for c in clean_title if unicodedata.category(c) != 'Mn')
        # Handle mathematical expressions like "2+2=5" -> "22-5" (remove operators but keep numbers together)
        clean_title = re.sub(r'(\d)\+(\d)', r'\1\2', clean_title)  # "2+2" -> "22"
        clean_title = re.sub(r'(\d)=(\d)', r'\1-\2', clean_title)  # "22=5" -> "22-5"
        slug = re.sub(r'[^a-zA-Z0-9]+', '-', clean_title.lower())
        # Remove leading/trailing hyphens
        slug = slug.strip('-')
        
        # Add year if present
        if year:
            slug = f"{slug}-{year}"
        
        final_url = f"https://letterboxd.com/film/{slug}/"
        
        
        return final_url
    
    async def scrape_alamo_drafthouse_async(self) -> List[Dict]:
        """Scrape Alamo Drafthouse NYC using Playwright"""
        movies = []
        
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
                        self.log("Found 'Load more' button, clicking...")
                        await load_more_button.first.click()
                        await page.wait_for_timeout(2000)  # Wait for new content to load
                except Exception as e:
                    self.log(f"No 'Load more' button found or error clicking: {e}")
                
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
                # print(f"movie data = {movie_data}")
                
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
                
                self.log(f"Found {len(movies)} movies at Alamo Drafthouse")
                
        except Exception as e:
            self.log(f"Error scraping Alamo with Playwright: {e}")
        
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
            # print(soup)
            
            # Look for movie titles in h3.movie_title a elements
            for title_elem in soup.select('h3.movie_title a'):
                title = title_elem.text.strip()
                if title:
                    movies.append({
                        'title': title,
                        'venue': 'Metrograph',
                        'url': 'https://metrograph.com' + title_elem.get('href', ''),
                        'source': 'metrograph',
                        'letterboxd_url': self.generate_letterboxd_url(title)
                    })
        except Exception as e:
            self.log(f"Error scraping Metrograph: {e}")
        
        return movies
    
    def scrape_ifc_center(self) -> List[Dict]:
        """Scrape IFC Center"""
        movies = []
        url = "https://www.ifccenter.com/"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'lxml')
            # print(soup)
            # Look for movie titles only in the "Now Playing" section
            now_playing_section = soup.select_one('.ifc-now-playing')
            if now_playing_section:
                grid_items = now_playing_section.select('.ifc-grid-item')
                
                for i, item in enumerate(grid_items):
                    title_elem = item.select_one('.ifc-grid-info h2')
                    link_elem = item.select_one('a[href]')
                    if title_elem and link_elem:
                        title = title_elem.text.strip()
                        
                       
                        movies.append({
                            'title': title,
                            'venue': 'IFC Center',
                            'url': link_elem.get('href', ''),
                            'source': 'ifc',
                            'letterboxd_url': self.generate_letterboxd_url(title)
                        })
                        
            
        except Exception as e:
            self.log(f"Error scraping IFC: {e}")
        
        return movies
    
    async def scrape_angelika_async(self) -> List[Dict]:
        """Scrape Angelika Film Center NYC using Playwright"""
        movies = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto('https://angelikafilmcenter.com/nyc/now-playing', wait_until='networkidle')
                
                # Wait for movie content to load
                await page.wait_for_timeout(3000)
                
                # Click the ANYTIME filter first to show all movies
                try:
                    anytime_button = page.locator('.common-filter:has-text("ANYTIME")')
                    if await anytime_button.count() > 0:
                        self.log("Found 'ANYTIME' filter button, clicking...")
                        await anytime_button.click()
                        await page.wait_for_timeout(2000)  # Wait for filter to apply
                        self.log("Clicked 'ANYTIME' filter successfully")
                    else:
                        self.log("No 'ANYTIME' filter button found")
                except Exception as e:
                    self.log(f"Error clicking ANYTIME filter: {e}")
                
                # Look for and click "show more" button
                try:
                    # The show more button appears as: <div class="show-more"><p>SHOW MORE</p></div>
                    # But after clicking it changes to: <div class="show-more"><p>SHOW LESS</p></div>
                    # Use .first to handle multiple buttons
                    show_more_button = page.locator('.show-more p:has-text("SHOW MORE")')
                    if await show_more_button.count() > 0:
                        self.log(f"Found {await show_more_button.count()} 'SHOW MORE' button(s), clicking first one...")
                        await show_more_button.first.click()
                        await page.wait_for_timeout(3000)  # Wait for new content to load
                        self.log("Clicked 'SHOW MORE' button successfully")
                    else:
                        # Check if button already shows "SHOW LESS" (meaning all content is already loaded)
                        show_less_button = page.locator('.show-more p:has-text("SHOW LESS")')
                        if await show_less_button.count() > 0:
                            self.log("All movies already showing (SHOW LESS button present)")
                        else:
                            self.log("No 'SHOW MORE' button found")
                        
                except Exception as e:
                    self.log(f"Error with show more button: {e}")
                
                # Extract movie information using the correct selectors
                movie_data = await page.evaluate('''
                    () => {
                        const movies = [];
                        
                        // Find all movie cards in the showtime section
                        const movieCards = document.querySelectorAll('.showtime-section-thumbnail .card__wrap--inner.angelika-film-center .card');
                        
                        movieCards.forEach(card => {
                            // Get the title from the h3 element
                            const titleElement = card.querySelector('.card__item.flexible h3');
                            // Get the link from the main card link or buy tickets link
                            const linkElement = card.querySelector('a[href*="/movies/details/"]') || 
                                              card.querySelector('.btn-border-danger-new[href*="/movies/details/"]');
                            
                            if (titleElement) {
                                const title = titleElement.textContent?.trim();
                                const link = linkElement ? linkElement.href : '';
                                
                                if (title) {
                                    movies.push({
                                        title: title,
                                        url: link,
                                        selector: 'angelika-card'
                                    });
                                }
                            }
                        });
                        
                        return movies;
                    }
                ''')
                
                await browser.close()
                
                # Process and deduplicate movie data
                seen_titles = set()
                for item in movie_data:
                    title = item['title'].strip()
                    if title and title.lower() not in seen_titles:
                        seen_titles.add(title.lower())
                        movies.append({
                            'title': title,
                            'venue': 'Angelika Film Center',
                            'url': item['url'] if item['url'].startswith('http') else f"https://angelikafilmcenter.com{item['url']}" if item['url'] else '',
                            'source': 'angelika',
                            'letterboxd_url': self.generate_letterboxd_url(title)
                        })
                
                self.log(f"Found {len(movies)} movies at Angelika Film Center")
                
        except Exception as e:
            self.log(f"Error scraping Angelika with Playwright: {e}")
        
        return movies
    
    def scrape_angelika(self) -> List[Dict]:
        """Scrape Angelika Film Center NYC - wrapper for async method"""
        return asyncio.run(self.scrape_angelika_async())
    
    async def scrape_angelika_village_east_async(self) -> List[Dict]:
        """Scrape Angelika Village East using Playwright"""
        movies = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto('https://angelikafilmcenter.com/villageeast/now-playing', wait_until='networkidle')
                
                # Wait for movie content to load
                await page.wait_for_timeout(3000)
                
                # Click the ANYTIME filter first to show all movies
                try:
                    anytime_button = page.locator('.common-filter:has-text("ANYTIME")')
                    if await anytime_button.count() > 0:
                        self.log("Found 'ANYTIME' filter button, clicking...")
                        await anytime_button.click()
                        await page.wait_for_timeout(2000)  # Wait for filter to apply
                        self.log("Clicked 'ANYTIME' filter successfully")
                    else:
                        self.log("No 'ANYTIME' filter button found")
                except Exception as e:
                    self.log(f"Error clicking ANYTIME filter: {e}")
                
                # Look for and click "show more" button
                try:
                    # The show more button appears as: <div class="show-more"><p>SHOW MORE</p></div>
                    # But after clicking it changes to: <div class="show-more"><p>SHOW LESS</p></div>
                    # Use .first to handle multiple buttons
                    show_more_button = page.locator('.show-more p:has-text("SHOW MORE")')
                    if await show_more_button.count() > 0:
                        self.log(f"Found {await show_more_button.count()} 'SHOW MORE' button(s), clicking first one...")
                        await show_more_button.first.click()
                        await page.wait_for_timeout(3000)  # Wait for new content to load
                        self.log("Clicked 'SHOW MORE' button successfully")
                    else:
                        # Check if button already shows "SHOW LESS" (meaning all content is already loaded)
                        show_less_button = page.locator('.show-more p:has-text("SHOW LESS")')
                        if await show_less_button.count() > 0:
                            self.log("All movies already showing (SHOW LESS button present)")
                        else:
                            self.log("No 'SHOW MORE' button found")
                        
                except Exception as e:
                    self.log(f"Error with show more button: {e}")
                
                # Extract movie information using the correct selectors
                movie_data = await page.evaluate('''
                    () => {
                        const movies = [];
                        
                        // Find all movie cards in the showtime section
                        const movieCards = document.querySelectorAll('.showtime-section-thumbnail .card__wrap--inner.angelika-film-center .card');
                        
                        movieCards.forEach(card => {
                            // Get the title from the h3 element
                            const titleElement = card.querySelector('.card__item.flexible h3');
                            // Get the link from the main card link or buy tickets link
                            const linkElement = card.querySelector('a[href*="/movies/details/"]') || 
                                              card.querySelector('.btn-border-danger-new[href*="/movies/details/"]');
                            
                            if (titleElement) {
                                const title = titleElement.textContent?.trim();
                                const link = linkElement ? linkElement.href : '';
                                
                                if (title) {
                                    movies.push({
                                        title: title,
                                        url: link,
                                        selector: 'angelika-card'
                                    });
                                }
                            }
                        });
                        
                        return movies;
                    }
                ''')
                
                await browser.close()
                
                # Process and deduplicate movie data
                seen_titles = set()
                for item in movie_data:
                    title = item['title'].strip()
                    if title and title.lower() not in seen_titles:
                        seen_titles.add(title.lower())
                        movies.append({
                            'title': title,
                            'venue': 'Angelika Village East',
                            'url': item['url'] if item['url'].startswith('http') else f"https://angelikafilmcenter.com{item['url']}" if item['url'] else '',
                            'source': 'angelika_village_east',
                            'letterboxd_url': self.generate_letterboxd_url(title)
                        })
                
                print(f"Found {len(movies)} movies at Angelika Village East")
                
        except Exception as e:
            print(f"Error scraping Angelika Village East with Playwright: {e}")
        
        return movies
    
    def scrape_angelika_village_east(self) -> List[Dict]:
        """Scrape Angelika Village East - wrapper for async method"""
        return asyncio.run(self.scrape_angelika_village_east_async())
    
    async def scrape_paris_theater_async(self) -> List[Dict]:
        """Scrape Paris Theater special engagements using Playwright"""
        movies = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto('https://www.paristheaternyc.com/special-engagements', wait_until='networkidle')
                
                # Wait for content to load
                await page.wait_for_timeout(3000)
                
                # Extract special engagement movies using the correct selectors
                movie_data = await page.evaluate('''
                    () => {
                        const movies = [];
                        
                        // Find all movie items in the special engagements grid
                        const movieCards = document.querySelectorAll('.special_engagements_all_films_grid_item__ufQRg .special_engagements_all_films_grid_item_container__GGii_');
                        
                        movieCards.forEach(card => {
                            // Get the title from the special_engagements_title__AocDK div
                            const titleElement = card.querySelector('.special_engagements_title__AocDK a');
                            // Get the dates
                            const dateElement = card.querySelector('.special_engagements_date__qHETy');
                            // Get the link to details page
                            const linkElement = card.querySelector('.special_engagements_title__AocDK a') || 
                                              card.querySelector('.special_engagements_buttons__U2vke a[href*="/film/"]');
                            
                            if (titleElement) {
                                const title = titleElement.textContent?.trim();
                                const date = dateElement ? dateElement.textContent?.trim() : '';
                                const link = linkElement ? linkElement.href : '';
                                
                                if (title) {
                                    movies.push({
                                        title: title,
                                        date: date,
                                        url: link,
                                        selector: 'paris-special-engagement'
                                    });
                                }
                            }
                        });
                        
                        return movies;
                    }
                ''')
                
                await browser.close()
                
                # Process movie data
                seen_titles = set()
                for item in movie_data:
                    title = item['title'].strip()
                    if title and title.lower() not in seen_titles:
                        seen_titles.add(title.lower())
                        
                        # Add date info to title if available for context
                        display_title = title
                        if item.get('date'):
                            display_title = f"{title} ({item['date']})"
                        
                        movies.append({
                            'title': title,  # Keep clean title for Letterboxd matching
                            'display_title': display_title,  # Title with dates for display
                            'venue': 'Paris Theater',
                            'url': item['url'] if item['url'].startswith('http') else f"https://www.paristheaternyc.com{item['url']}" if item['url'] else '',
                            'source': 'paris_theater',
                            'letterboxd_url': self.generate_letterboxd_url(title)
                        })
                
                print(f"Found {len(movies)} special engagement movies at Paris Theater")
                
        except Exception as e:
            print(f"Error scraping Paris Theater with Playwright: {e}")
        
        return movies
    
    def scrape_paris_theater(self) -> List[Dict]:
        """Scrape Paris Theater special engagements - wrapper for async method"""
        return asyncio.run(self.scrape_paris_theater_async())
    
    async def scrape_nitehawk_williamsburg_async(self) -> List[Dict]:
        """Scrape Nitehawk Cinema Williamsburg using Playwright"""
        movies = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto('https://nitehawkcinema.com/williamsburg', wait_until='networkidle')
                
                # Wait for dynamic content to load
                await page.wait_for_timeout(5000)
                
                # Extract movie information using the correct selectors
                movie_data = await page.evaluate('''
                    () => {
                        const movies = [];
                        
                        // Find all movie containers in the buy-tickets-listview
                        const movieContainers = document.querySelectorAll('#buy-tickets-listview .show-container');
                        
                        movieContainers.forEach(container => {
                            // Get the title from the show-title div
                            const titleElement = container.querySelector('.show-title');
                            // Get the description
                            const descElement = container.querySelector('.short-description');
                            // Get the link from overlay-link or details button
                            const linkElement = container.querySelector('.overlay-link') || 
                                              container.querySelector('a[href*="/movies/"]');
                            
                            if (titleElement) {
                                const title = titleElement.textContent?.trim();
                                const description = descElement ? descElement.textContent?.trim() : '';
                                const link = linkElement ? linkElement.href : '';
                                
                                if (title) {
                                    movies.push({
                                        title: title,
                                        description: description,
                                        url: link,
                                        selector: 'nitehawk-show-container'
                                    });
                                }
                            }
                        });
                        
                        return movies;
                    }
                ''')
                
                await browser.close()
                
                # Process and filter movie data
                seen_titles = set()
                for item in movie_data:
                    title = item['title'].strip()
                    if title and title.lower() not in seen_titles:
                        # Additional filtering for likely movie titles
                        if len(title) > 2 and not title.isdigit() and not title.startswith('#'):
                            seen_titles.add(title.lower())
                            movies.append({
                                'title': title,
                                'venue': 'Nitehawk Cinema Williamsburg',
                                'url': item['url'] if item['url'].startswith('http') else f"https://nitehawkcinema.com{item['url']}" if item['url'] else '',
                                'source': 'nitehawk_williamsburg',
                                'letterboxd_url': self.generate_letterboxd_url(title)
                            })
                
                print(f"Found {len(movies)} movies at Nitehawk Cinema Williamsburg")
                
        except Exception as e:
            print(f"Error scraping Nitehawk Williamsburg with Playwright: {e}")
        
        return movies
    
    def scrape_nitehawk_williamsburg(self) -> List[Dict]:
        """Scrape Nitehawk Cinema Williamsburg - wrapper for async method"""
        return asyncio.run(self.scrape_nitehawk_williamsburg_async())
    
    async def scrape_nitehawk_prospect_park_async(self) -> List[Dict]:
        """Scrape Nitehawk Cinema Prospect Park using Playwright"""
        movies = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto('https://nitehawkcinema.com/prospectpark', wait_until='networkidle')
                
                # Wait for dynamic content to load
                await page.wait_for_timeout(5000)
                
                # Extract movie information using the correct selectors (same as Williamsburg)
                movie_data = await page.evaluate('''
                    () => {
                        const movies = [];
                        
                        // Find all movie containers in the buy-tickets-listview
                        const movieContainers = document.querySelectorAll('#buy-tickets-listview .show-container');
                        
                        movieContainers.forEach(container => {
                            // Get the title from the show-title div
                            const titleElement = container.querySelector('.show-title');
                            // Get the description
                            const descElement = container.querySelector('.short-description');
                            // Get the link from overlay-link or details button
                            const linkElement = container.querySelector('.overlay-link') || 
                                              container.querySelector('a[href*="/movies/"]');
                            
                            if (titleElement) {
                                const title = titleElement.textContent?.trim();
                                const description = descElement ? descElement.textContent?.trim() : '';
                                const link = linkElement ? linkElement.href : '';
                                
                                if (title) {
                                    movies.push({
                                        title: title,
                                        description: description,
                                        url: link,
                                        selector: 'nitehawk-show-container'
                                    });
                                }
                            }
                        });
                        
                        return movies;
                    }
                ''')
                
                await browser.close()
                
                # Process and filter movie data
                seen_titles = set()
                for item in movie_data:
                    title = item['title'].strip()
                    if title and title.lower() not in seen_titles:
                        # Additional filtering for likely movie titles
                        if len(title) > 2 and not title.isdigit() and not title.startswith('#'):
                            seen_titles.add(title.lower())
                            movies.append({
                                'title': title,
                                'venue': 'Nitehawk Cinema Prospect Park',
                                'url': item['url'] if item['url'].startswith('http') else f"https://nitehawkcinema.com{item['url']}" if item['url'] else '',
                                'source': 'nitehawk_prospect_park',
                                'letterboxd_url': self.generate_letterboxd_url(title)
                            })
                
                print(f"Found {len(movies)} movies at Nitehawk Cinema Prospect Park")
                
        except Exception as e:
            print(f"Error scraping Nitehawk Prospect Park with Playwright: {e}")
        
        return movies
    
    def scrape_nitehawk_prospect_park(self) -> List[Dict]:
        """Scrape Nitehawk Cinema Prospect Park - wrapper for async method"""
        return asyncio.run(self.scrape_nitehawk_prospect_park_async())
    
    async def scrape_moving_image_async(self) -> List[Dict]:
        """Scrape Museum of the Moving Image film events using Playwright"""
        movies = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # Set user agent to avoid bot detection
                await page.set_extra_http_headers({
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                })
                
                await page.goto('https://movingimage.org/events/list/?tribe_filterbar_category_custom%5B0%5D=230', wait_until='networkidle')
                
                # Wait for content to load
                await page.wait_for_timeout(3000)
                
                # Extract movie event information using the correct selectors
                movie_data = await page.evaluate('''
                    () => {
                        const events = [];
                        
                        // Look for event rows in the tribe-events-calendar-list
                        const eventRows = document.querySelectorAll('.tribe-events-calendar-list__event-row');
                        
                        eventRows.forEach(row => {
                            // Get the event article within this row
                            const eventArticle = row.querySelector('.tribe-events-calendar-list__event');
                            
                            if (eventArticle) {
                                // Get title from the h3 link
                                const titleElement = eventArticle.querySelector('.tribe-events-calendar-list__event-title a');
                                
                                // Get date/time from the datetime wrapper
                                const dateElement = eventArticle.querySelector('.tribe-events-calendar-day__event-datetime-wrapper time');
                                
                                // Get description from the event description
                                const descElement = eventArticle.querySelector('.tribe-events-calendar-list__event-description');
                                
                                if (titleElement) {
                                    const title = titleElement.textContent?.trim();
                                    const date = dateElement ? dateElement.textContent?.trim() : '';
                                    const description = descElement ? descElement.textContent?.trim() : '';
                                    const link = titleElement.href || '';
                                    
                                    if (title && title.length > 3) {
                                        events.push({
                                            title: title,
                                            date: date,
                                            description: description,
                                            url: link,
                                            selector: 'tribe-events-calendar-list'
                                        });
                                    }
                                }
                            }
                        });
                        
                        return events;
                    }
                ''')
                
                await browser.close()
                
                # Process and filter movie data
                seen_titles = set()
                for item in movie_data:
                    title = item['title'].strip()
                    if title and title.lower() not in seen_titles:
                        # Filter out clearly non-movie events
                        if not any(exclude in title.lower() for exclude in ['workshop', 'discussion', 'panel', 'lecture', 'tour', 'class', 'exhibition']):
                            seen_titles.add(title.lower())
                            
                            # Clean title for Letterboxd matching - remove date suffixes and event info
                            clean_title = title
                            # Remove date patterns like "December 15, 2024" or "Dec 15"
                            import re
                            clean_title = re.sub(r'\s*-?\s*(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,?\s+\d{4})?', '', clean_title)
                            clean_title = re.sub(r'\s*-?\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}(?:,?\s+\d{4})?', '', clean_title)
                            # Remove time patterns like "7:00 PM" or "at 7pm"
                            clean_title = re.sub(r'\s*-?\s*(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)', '', clean_title)
                            clean_title = re.sub(r'\s*-?\s*(?:at\s+)?\d{1,2}\s*(?:AM|PM|am|pm)', '', clean_title)
                            # Remove parenthetical notes at end of string like "(with live piano)", "(3D)", etc.
                            clean_title = re.sub(r'\s*\([^)]*\)$', '', clean_title)
                            # Remove "3D" suffix
                            clean_title = re.sub(r'\s+3D$', '', clean_title)
                            clean_title = clean_title.strip(' -')
                            
                            movies.append({
                                'title': clean_title,
                                'venue': 'Museum of the Moving Image',
                                'url': item['url'] if item['url'].startswith('http') else f"https://movingimage.org{item['url']}" if item['url'] else '',
                                'source': 'moving_image',
                                'letterboxd_url': self.generate_letterboxd_url(clean_title)
                            })
                
                print(f"Found {len(movies)} film events at Museum of the Moving Image")
                
        except Exception as e:
            print(f"Error scraping Museum of the Moving Image with Playwright: {e}")
        
        return movies
    
    def scrape_moving_image(self) -> List[Dict]:
        """Scrape Museum of the Moving Image - wrapper for async method"""
        return asyncio.run(self.scrape_moving_image_async())
    
    async def scrape_film_forum_async(self) -> List[Dict]:
        """Scrape Film Forum using Playwright"""
        movies = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # Set user agent to avoid bot detection
                await page.set_extra_http_headers({
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                })
                
                # Try to go to the page with a longer timeout
                try:
                    await page.goto('https://filmforum.org/now_playing', wait_until='networkidle', timeout=60000)
                except Exception as goto_error:
                    print(f"Film Forum: Failed to load page with networkidle, trying domcontentloaded: {goto_error}")
                    try:
                        await page.goto('https://filmforum.org/now_playing', wait_until='domcontentloaded', timeout=30000)
                    except Exception as fallback_error:
                        print(f"Film Forum: Failed to load page entirely: {fallback_error}")
                        await browser.close()
                        return movies
                
                # Wait for content to load
                await page.wait_for_timeout(5000)
                
                # Extract movie information using the correct selectors
                movie_data = await page.evaluate('''
                    () => {
                        const movies = [];
                        
                        // Look for movie details in film-details divs
                        const filmDetailsElements = document.querySelectorAll('.film-details');
                        
                        filmDetailsElements.forEach(element => {
                            // Get title from the .title.style-a a element
                            const titleElement = element.querySelector('.title.style-a a');
                            
                            // Get any additional details
                            const detailsElement = element.querySelector('.details p');
                            
                            if (titleElement) {
                                const title = titleElement.textContent?.trim();
                                const url = titleElement.href || '';
                                const details = detailsElement ? detailsElement.textContent?.trim() : '';
                                
                                if (title && title.length > 2) {
                                    movies.push({
                                        title: title,
                                        details: details,
                                        url: url,
                                        selector: 'film-forum-details'
                                    });
                                }
                            }
                        });
                        
                        return movies;
                    }
                ''')
                
                await browser.close()
                
                # Process and filter movie data
                seen_titles = set()
                for item in movie_data:
                    title = item['title'].strip()
                    if title and title.lower() not in seen_titles:
                        seen_titles.add(title.lower())
                        
                        # Clean title for Letterboxd matching - remove director prefixes and format suffixes
                        clean_title = title
                        import re
                        # Remove director prefixes like "John Schlesinger's", "Zhang Yimou's", etc.
                        clean_title = re.sub(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\'s\s+', '', clean_title)
                        clean_title = re.sub(r'^[A-Z]\.?[A-Z]\.?\s+[A-Z][a-z]+\'s\s+', '', clean_title)  # "G.W. Pabst's"
                        clean_title = re.sub(r'^[A-Z][a-z]+\s+[A-Z][a-z]+\'s\s+', '', clean_title)  # "Cecil B. DeMille's"
                        # Remove <br> tags
                        clean_title = re.sub(r'\s*<br>\s*', ' ', clean_title)
                        # Remove format suffixes like "in 35mm"
                        clean_title = re.sub(r'\s*in\s+\d+mm$', '', clean_title, flags=re.IGNORECASE)
                        # Remove trailing spaces and other cleanup
                        clean_title = clean_title.strip()
                        
                        movies.append({
                            'title': clean_title,
                            'venue': 'Film Forum',
                            'url': item['url'] if item['url'].startswith('http') else f"https://filmforum.org{item['url']}" if item['url'] else '',
                            'source': 'film_forum',
                            'letterboxd_url': self.generate_letterboxd_url(clean_title)
                        })
                
                print(f"Found {len(movies)} films at Film Forum")
                
        except Exception as e:
            print(f"Error scraping Film Forum with Playwright: {e}")
        
        return movies
    
    def scrape_film_forum(self) -> List[Dict]:
        """Scrape Film Forum - wrapper for async method"""
        return asyncio.run(self.scrape_film_forum_async())
    
    def get_all_movies(self, selected_theaters=None) -> List[Dict]:
        """Aggregate movies from selected sources"""
        if selected_theaters is None:
            # Default to all theaters if none specified
            selected_theaters = ['alamo', 'metrograph', 'ifc', 'angelika', 'angelika_village_east', 
                               'paris_theater', 'nitehawk_williamsburg', 'nitehawk_prospect_park', 
                               'moving_image', 'film_forum']
        
        all_movies = []
        theater_scrapers = {
            'alamo': self.scrape_alamo_drafthouse,
            'metrograph': self.scrape_metrograph,
            'ifc': self.scrape_ifc_center,
            'angelika': self.scrape_angelika,
            'angelika_village_east': self.scrape_angelika_village_east,
            'paris_theater': self.scrape_paris_theater,
            'nitehawk_williamsburg': self.scrape_nitehawk_williamsburg,
            'nitehawk_prospect_park': self.scrape_nitehawk_prospect_park,
            'moving_image': self.scrape_moving_image,
            'film_forum': self.scrape_film_forum
        }
        
        for theater_id in selected_theaters:
            if theater_id in theater_scrapers:
                # Check cache first (only if use_cache is True)
                cached_movies = self._get_cached_movies(theater_id) if self.use_cache else []
                if cached_movies and self.use_cache:
                    self.log(f"📂 Using cached data for {theater_id.replace('_', ' ').title()} ({len(cached_movies)} movies)")
                    all_movies.extend(cached_movies)
                else:
                    if not self.use_cache:
                        self.log(f"🔄 Cache disabled - Scraping {theater_id.replace('_', ' ').title()}...")
                    else:
                        self.log(f"🎭 Scraping {theater_id.replace('_', ' ').title()}...")
                    try:
                        movies = theater_scrapers[theater_id]()
                        all_movies.extend(movies)
                        # Cache the results (only if use_cache is True)
                        if self.use_cache:
                            self._cache_movies(theater_id, movies)
                            self.log(f"💾 Cached {len(movies)} movies for {theater_id.replace('_', ' ').title()}")
                    except Exception as e:
                        self.log(f"❌ Error scraping {theater_id}: {e}")
        
        # Deduplicate by Letterboxd URL and collect all sources
        movie_dict = {}
        for movie in all_movies:
            letterboxd_url = movie['letterboxd_url']
            if letterboxd_url not in movie_dict:
                # First time seeing this movie - initialize with sources as list
                movie_dict[letterboxd_url] = movie.copy()
                movie_dict[letterboxd_url]['sources'] = [movie['source']]
            else:
                # Movie already exists - add source to list if not already there
                if movie['source'] not in movie_dict[letterboxd_url]['sources']:
                    movie_dict[letterboxd_url]['sources'].append(movie['source'])
                # Update venue list if different venues show the same movie
                existing_venue = movie_dict[letterboxd_url]['venue']
                if movie['venue'] not in existing_venue:
                    movie_dict[letterboxd_url]['venue'] = f"{existing_venue}, {movie['venue']}"
        
        deduplicated_movies = list(movie_dict.values())
        self.log(f"📊 Deduplicated from {len(all_movies)} to {len(deduplicated_movies)} unique movies")
        return deduplicated_movies
