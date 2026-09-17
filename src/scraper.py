import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict
import asyncio
import json
import os
import pytz
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

try:
    from . import db
except ImportError:
    import db

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
        """Load theater cache from Supabase (if configured) or local JSON file"""
        if db.is_enabled():
            # 7-day TTL cleanup: keep recent showings so the app can display
            # the latest cached data on startup even if it isn't from today
            week_ago = (datetime.now(self.eastern_tz) - timedelta(days=7)).strftime('%Y-%m-%d')
            db.delete_stale_showings(week_ago)
            self.theater_cache = db.get_latest_showings()
            if self.theater_cache:
                self.log(f"📂 Loaded theater cache from Supabase with {len(self.theater_cache)} entries")
            return

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
        date_str = self._get_eastern_date_string()
        self.theater_cache[theater_id] = {
            'date': date_str,
            'movies': movies,
            'cached_at': datetime.now(self.eastern_tz).isoformat()
        }
        if db.is_enabled():
            db.upsert_theater_showings(theater_id, date_str, movies)
        else:
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

    async def _goto_and_wait(self, page, url: str, content_selector: str,
                             goto_timeout: int = 30000, selector_timeout: int = 15000):
        """Navigate and wait for real content instead of flaky 'networkidle'.

        Uses domcontentloaded, then waits for the selector the scraper actually
        extracts from. Falls back gracefully if the selector never appears.
        """
        await page.goto(url, wait_until='domcontentloaded', timeout=goto_timeout)
        try:
            await page.wait_for_selector(content_selector, timeout=selector_timeout)
        except PWTimeoutError:
            self.log(f"⚠️ Content selector not found at {url}; extracting whatever loaded")
            await page.wait_for_timeout(2000)

    _MONTHS = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
               'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}

    def _month_day_to_iso(self, month_str: str, day_str: str):
        """('Sep', '17') -> '2026-09-17'. Infers the year (rolls over near Dec/Jan)."""
        from datetime import date
        month = self._MONTHS.get((month_str or '').strip().lower()[:3])
        try:
            day = int(day_str)
        except (TypeError, ValueError):
            return None
        if not month:
            return None
        today = datetime.now(self.eastern_tz).date()
        for year in (today.year, today.year + 1):
            try:
                candidate = date(year, month, day)
            except ValueError:
                continue
            if candidate >= today - timedelta(days=45):
                return candidate.isoformat()
        return None

    def _parse_date_range_text(self, text: str) -> List[str]:
        """Parse 'OCT 3 — OCT 9' / 'SEP 26' style text into a list of ISO dates."""
        import re
        matches = re.findall(r'([A-Za-z]{3,9})\.?\s+(\d{1,2})', text or '')
        isos = []
        for month_str, day_str in matches:
            iso = self._month_day_to_iso(month_str, day_str)
            if iso:
                isos.append(iso)
        if len(isos) >= 2:
            start = datetime.strptime(isos[0], '%Y-%m-%d').date()
            end = datetime.strptime(isos[-1], '%Y-%m-%d').date()
            if start <= end and (end - start).days <= 90:
                return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
        return sorted(set(isos))

    def _format_dates_display(self, dates: List[str]) -> str:
        """Compact display for ISO dates: 'Sep 17–23, Oct 1'."""
        try:
            objs = sorted({datetime.strptime(d, '%Y-%m-%d').date() for d in dates})
        except ValueError:
            return ''
        if not objs:
            return ''
        ranges = []
        start = prev = objs[0]
        for d in objs[1:]:
            if (d - prev).days == 1:
                prev = d
                continue
            ranges.append((start, prev))
            start = prev = d
        ranges.append((start, prev))
        parts = []
        for a, b in ranges:
            if a == b:
                parts.append(f"{a.strftime('%b')} {a.day}")
            elif a.month == b.month:
                parts.append(f"{a.strftime('%b')} {a.day}–{b.day}")
            else:
                parts.append(f"{a.strftime('%b')} {a.day} – {b.strftime('%b')} {b.day}")
        return ', '.join(parts)

    def scrape_alamo_drafthouse(self) -> List[Dict]:
        """Scrape Alamo Drafthouse NYC via its public schedule API (includes show dates)"""
        movies = []
        try:
            response = requests.get(
                'https://drafthouse.com/s/mother/v2/schedule/market/nyc',
                headers=self.headers, timeout=15
            )
            data = response.json()['data']

            # Map presentation slug -> set of dates with bookable sessions
            dates_by_slug = {}
            for session in data.get('sessions', []):
                slug = session.get('presentationSlug')
                date_str = session.get('businessDateClt')  # 'YYYY-MM-DD'
                if slug and date_str:
                    dates_by_slug.setdefault(slug, set()).add(date_str)

            for presentation in data.get('presentations', []):
                slug = presentation.get('slug')
                title = (presentation.get('show') or {}).get('title')
                if not title or slug not in dates_by_slug:
                    continue  # skip announced titles with no sessions on sale
                movies.append({
                    'title': title,
                    'venue': 'Alamo Drafthouse',
                    'url': f'https://drafthouse.com/nyc/show/{slug}',
                    'source': 'alamo',
                    'letterboxd_url': self.generate_letterboxd_url(title),
                    'show_dates': sorted(dates_by_slug[slug]),
                })

            self.log(f"Found {len(movies)} movies at Alamo Drafthouse")
        except Exception as e:
            self.log(f"Error scraping Alamo schedule API: {e}")

        return movies

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
                    # Dates live in sibling .film_day divs with ids like "day_Fri_Sep_18"
                    show_dates = set()
                    container = title_elem.find_parent('div', class_='col-sm-6')
                    if container:
                        for day_div in container.select('.film_day[id^="day_"]'):
                            parts = day_div.get('id', '').split('_')  # ['day', 'Fri', 'Sep', '18']
                            if len(parts) == 4:
                                iso = self._month_day_to_iso(parts[2], parts[3])
                                if iso:
                                    show_dates.add(iso)

                    movies.append({
                        'title': title,
                        'venue': 'Metrograph',
                        'url': 'https://metrograph.com' + title_elem.get('href', ''),
                        'source': 'metrograph',
                        'letterboxd_url': self.generate_letterboxd_url(title),
                        'show_dates': sorted(show_dates)
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

            # The weekly schedule widget maps each film to the days it screens
            # (.daily-schedule divs: h3 "Thu Sep 17" + film links per day)
            dates_by_title = {}
            for day_div in soup.select('.daily-schedule'):
                if 'show-coming-soon' in (day_div.get('class') or []):
                    continue
                h3 = day_div.select_one('h3')
                parts = h3.text.strip().split() if h3 else []  # ['Thu', 'Sep', '17']
                iso = self._month_day_to_iso(parts[1], parts[2]) if len(parts) == 3 else None
                if not iso:
                    continue
                for link in day_div.select('.details h3 a'):
                    key = link.text.strip().lower().replace('\u2019', "'")
                    dates_by_title.setdefault(key, set()).add(iso)

            # Look for movie titles only in the "Now Playing" section
            now_playing_section = soup.select_one('.ifc-now-playing')
            if now_playing_section:
                grid_items = now_playing_section.select('.ifc-grid-item')

                for i, item in enumerate(grid_items):
                    title_elem = item.select_one('.ifc-grid-info h2')
                    link_elem = item.select_one('a[href]')
                    if title_elem and link_elem:
                        title = title_elem.text.strip()

                        # Match schedule entries, including variants like "Title (Open Captioning)"
                        title_key = title.lower().replace('\u2019', "'")
                        show_dates = set(dates_by_title.get(title_key, set()))
                        for key, day_set in dates_by_title.items():
                            if key.startswith(title_key + ' ('):
                                show_dates |= day_set

                        movies.append({
                            'title': title,
                            'venue': 'IFC Center',
                            'url': link_elem.get('href', ''),
                            'source': 'ifc',
                            'letterboxd_url': self.generate_letterboxd_url(title),
                            'show_dates': sorted(show_dates)
                        })
                        
            
        except Exception as e:
            self.log(f"Error scraping IFC: {e}")
        
        return movies
    
    async def scrape_angelika_async(self, browser) -> List[Dict]:
        """Scrape Angelika Film Center NYC using Playwright"""
        movies = []

        try:
            context = await browser.new_context()
            try:
                page = await context.new_page()

                # Movie cards only render after the ANYTIME filter click,
                # so wait for the filter bar (or cards, if already present)
                await self._goto_and_wait(
                    page,
                    'https://angelikafilmcenter.com/nyc/now-playing',
                    '.common-filter, .showtime-section-thumbnail .card'
                )
                
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
            finally:
                await context.close()
        except Exception as e:
            self.log(f"Error scraping Angelika with Playwright: {e}")

        return movies

    async def scrape_angelika_village_east_async(self, browser) -> List[Dict]:
        """Scrape Angelika Village East using Playwright"""
        movies = []

        try:
            context = await browser.new_context()
            try:
                page = await context.new_page()

                # Movie cards only render after the ANYTIME filter click,
                # so wait for the filter bar (or cards, if already present)
                await self._goto_and_wait(
                    page,
                    'https://angelikafilmcenter.com/villageeast/now-playing',
                    '.common-filter, .showtime-section-thumbnail .card'
                )
                
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
                
                self.log(f"Found {len(movies)} movies at Angelika Village East")
            finally:
                await context.close()
        except Exception as e:
            self.log(f"Error scraping Angelika Village East with Playwright: {e}")

        return movies

    async def scrape_paris_theater_async(self, browser) -> List[Dict]:
        """Scrape Paris Theater special engagements using Playwright"""
        movies = []

        try:
            context = await browser.new_context()
            try:
                page = await context.new_page()

                await self._goto_and_wait(
                    page,
                    'https://www.paristheaternyc.com/special-engagements',
                    'div[class*="special_engagements_all_films_grid_item"]'
                )
                
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
                            'letterboxd_url': self.generate_letterboxd_url(title),
                            'show_dates': self._parse_date_range_text(item.get('date', ''))
                        })
                
                self.log(f"Found {len(movies)} special engagement movies at Paris Theater")
            finally:
                await context.close()
        except Exception as e:
            self.log(f"Error scraping Paris Theater with Playwright: {e}")

        return movies

    async def scrape_nitehawk_williamsburg_async(self, browser) -> List[Dict]:
        """Scrape Nitehawk Cinema Williamsburg using Playwright"""
        movies = []

        try:
            context = await browser.new_context()
            try:
                page = await context.new_page()

                await self._goto_and_wait(
                    page,
                    'https://nitehawkcinema.com/williamsburg',
                    '#buy-tickets-listview .show-container'
                )
                # List is populated incrementally by JS - brief settle
                await page.wait_for_timeout(1000)
                
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
                
                self.log(f"Found {len(movies)} movies at Nitehawk Cinema Williamsburg")
            finally:
                await context.close()
        except Exception as e:
            self.log(f"Error scraping Nitehawk Williamsburg with Playwright: {e}")

        return movies

    async def scrape_nitehawk_prospect_park_async(self, browser) -> List[Dict]:
        """Scrape Nitehawk Cinema Prospect Park using Playwright"""
        movies = []

        try:
            context = await browser.new_context()
            try:
                page = await context.new_page()

                await self._goto_and_wait(
                    page,
                    'https://nitehawkcinema.com/prospectpark',
                    '#buy-tickets-listview .show-container'
                )
                # List is populated incrementally by JS - brief settle
                await page.wait_for_timeout(1000)
                
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
                
                self.log(f"Found {len(movies)} movies at Nitehawk Cinema Prospect Park")
            finally:
                await context.close()
        except Exception as e:
            self.log(f"Error scraping Nitehawk Prospect Park with Playwright: {e}")

        return movies

    async def scrape_moving_image_async(self, browser) -> List[Dict]:
        """Scrape Museum of the Moving Image film events using Playwright"""
        movies = []

        try:
            # Custom user agent to avoid bot detection
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            try:
                page = await context.new_page()

                await self._goto_and_wait(
                    page,
                    'https://movingimage.org/events/list/?tribe_filterbar_category_custom%5B0%5D=230',
                    '.tribe-events-calendar-list__event-row'
                )
                
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

                # Process and filter movie data (accumulate dates across repeat screenings)
                import re
                by_title = {}
                for item in movie_data:
                    title = item['title'].strip()
                    if not title:
                        continue
                    # Filter out clearly non-movie events
                    if any(exclude in title.lower() for exclude in ['workshop', 'discussion', 'panel', 'lecture', 'tour', 'class', 'exhibition']):
                        continue

                    # Clean title for Letterboxd matching - remove date suffixes and event info
                    clean_title = title
                    # Remove date patterns like "December 15, 2024" or "Dec 15"
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

                    # Event date like "September 20 @ 2:00 pm"
                    show_dates = self._parse_date_range_text(item.get('date', ''))

                    key = clean_title.lower()
                    if key in by_title:
                        by_title[key]['show_dates'] = sorted(set(by_title[key]['show_dates']) | set(show_dates))
                    else:
                        by_title[key] = {
                            'title': clean_title,
                            'venue': 'Museum of the Moving Image',
                            'url': item['url'] if item['url'].startswith('http') else f"https://movingimage.org{item['url']}" if item['url'] else '',
                            'source': 'moving_image',
                            'letterboxd_url': self.generate_letterboxd_url(clean_title),
                            'show_dates': show_dates
                        }
                movies.extend(by_title.values())
                
                self.log(f"Found {len(movies)} film events at Museum of the Moving Image")
            finally:
                await context.close()
        except Exception as e:
            self.log(f"Error scraping Museum of the Moving Image with Playwright: {e}")

        return movies

    async def scrape_film_forum_async(self, browser) -> List[Dict]:
        """Scrape Film Forum using Playwright"""
        movies = []

        try:
            # Custom user agent to avoid bot detection
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            try:
                page = await context.new_page()

                await self._goto_and_wait(
                    page,
                    'https://filmforum.org/now_playing',
                    '.film-details .title.style-a a'
                )
                
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
                
                self.log(f"Found {len(movies)} films at Film Forum")
            finally:
                await context.close()
        except Exception as e:
            self.log(f"Error scraping Film Forum with Playwright: {e}")

        return movies

    async def _scrape_all_async(self, theater_ids: List[str]) -> Dict[str, List[Dict]]:
        """Scrape all requested theaters concurrently, sharing one browser."""
        playwright_scrapers = {
            'angelika': self.scrape_angelika_async,
            'angelika_village_east': self.scrape_angelika_village_east_async,
            'paris_theater': self.scrape_paris_theater_async,
            'nitehawk_williamsburg': self.scrape_nitehawk_williamsburg_async,
            'nitehawk_prospect_park': self.scrape_nitehawk_prospect_park_async,
            'moving_image': self.scrape_moving_image_async,
            'film_forum': self.scrape_film_forum_async,
        }
        sync_scrapers = {
            'alamo': self.scrape_alamo_drafthouse,
            'metrograph': self.scrape_metrograph,
            'ifc': self.scrape_ifc_center,
        }

        sem = asyncio.Semaphore(2)  # cap concurrent pages (memory on Render)

        async def run_playwright(tid, coro_fn, browser):
            async with sem:
                self.log(f"🎭 Scraping {tid.replace('_', ' ').title()}...")
                try:
                    return await asyncio.wait_for(coro_fn(browser), timeout=90)
                except Exception as e:
                    self.log(f"❌ Error scraping {tid}: {e}")
                    return []

        async def run_sync(tid, fn):
            self.log(f"🎭 Scraping {tid.replace('_', ' ').title()}...")
            try:
                return await asyncio.to_thread(fn)
            except Exception as e:
                self.log(f"❌ Error scraping {tid}: {e}")
                return []

        tasks = {}
        needs_browser = any(tid in playwright_scrapers for tid in theater_ids)
        done = []
        if needs_browser:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=['--disable-dev-shm-usage'])
                try:
                    for tid in theater_ids:
                        if tid in playwright_scrapers:
                            tasks[tid] = run_playwright(tid, playwright_scrapers[tid], browser)
                        elif tid in sync_scrapers:
                            tasks[tid] = run_sync(tid, sync_scrapers[tid])
                    done = await asyncio.gather(*tasks.values(), return_exceptions=True)
                finally:
                    await browser.close()
        else:
            for tid in theater_ids:
                if tid in sync_scrapers:
                    tasks[tid] = run_sync(tid, sync_scrapers[tid])
            if tasks:
                done = await asyncio.gather(*tasks.values(), return_exceptions=True)

        return {tid: (res if isinstance(res, list) else []) for tid, res in zip(tasks.keys(), done)}

    def get_all_movies(self, selected_theaters=None) -> List[Dict]:
        """Aggregate movies from selected sources"""
        known_theaters = ['alamo', 'metrograph', 'ifc', 'angelika', 'angelika_village_east',
                          'paris_theater', 'nitehawk_williamsburg', 'nitehawk_prospect_park',
                          'moving_image', 'film_forum']
        if selected_theaters is None:
            # Default to all theaters if none specified
            selected_theaters = known_theaters

        all_movies = []
        to_scrape = []
        for theater_id in selected_theaters:
            if theater_id not in known_theaters:
                continue
            # Check cache first (only if use_cache is True)
            cached_movies = self._get_cached_movies(theater_id) if self.use_cache else []
            if cached_movies and self.use_cache:
                self.log(f"📂 Using cached data for {theater_id.replace('_', ' ').title()} ({len(cached_movies)} movies)")
                all_movies.extend(cached_movies)
            else:
                if not self.use_cache:
                    self.log(f"🔄 Cache disabled - will scrape {theater_id.replace('_', ' ').title()}")
                to_scrape.append(theater_id)

        if to_scrape:
            results = asyncio.run(self._scrape_all_async(to_scrape))
            for theater_id, movies in results.items():
                all_movies.extend(movies)
                # Cache the results (only if use_cache is True and the scrape succeeded)
                if self.use_cache and movies:
                    self._cache_movies(theater_id, movies)
                    self.log(f"💾 Cached {len(movies)} movies for {theater_id.replace('_', ' ').title()}")


        return self._dedupe_movies(all_movies)

    def _dedupe_movies(self, all_movies: List[Dict]) -> List[Dict]:
        """Deduplicate by Letterboxd URL, merging sources/venues/show dates."""
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
                # Merge show dates across venues/listings
                if movie.get('show_dates'):
                    existing_dates = movie_dict[letterboxd_url].get('show_dates') or []
                    movie_dict[letterboxd_url]['show_dates'] = sorted(set(existing_dates) | set(movie['show_dates']))

        deduplicated_movies = list(movie_dict.values())
        for movie in deduplicated_movies:
            if movie.get('show_dates'):
                movie['dates_display'] = self._format_dates_display(movie['show_dates'])
        self.log(f"📊 Deduplicated from {len(all_movies)} to {len(deduplicated_movies)} unique movies")
        return deduplicated_movies

    def get_cached_movies_only(self, max_age_days: int = 7) -> tuple:
        """Return (movies, newest_cached_at) from the most recent cached
        showings, accepting entries up to max_age_days old. Never scrapes.

        If cached data exists but is all older than max_age_days, returns
        ([], newest_stale_cached_at) so callers can tell 'stale' from 'empty'
        (([], None) means no cached data at all)."""
        cutoff = (datetime.now(self.eastern_tz) - timedelta(days=max_age_days)).strftime('%Y-%m-%d')
        all_movies = []
        newest_cached_at = None
        for theater_id, entry in self.theater_cache.items():
            cached_at = entry.get('cached_at')
            if cached_at and (newest_cached_at is None or str(cached_at) > str(newest_cached_at)):
                newest_cached_at = cached_at
            if (entry.get('date') or '') >= cutoff:
                all_movies.extend(entry.get('movies') or [])
        if not all_movies:
            return [], newest_cached_at
        return self._dedupe_movies(all_movies), newest_cached_at
