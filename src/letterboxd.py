import requests
from bs4 import BeautifulSoup
import re
import time
import json
import asyncio
import csv
import os
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class LetterboxdAPI:
    """Fetch Letterboxd ratings for movies"""
    
    def __init__(self):
        self.base_url = "https://letterboxd.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.cache = {}
        self.movies_found_no_rating = []
        self.cache_file = 'letterboxd_cache.csv'
        self.csv_cache = {}
        self._load_csv_cache()
        self._lock = threading.Lock()  # For thread-safe operations
    
    def _load_csv_cache(self):
        """Load existing cache from CSV file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        letterboxd_url = row['letterboxd_url']
                        rating = float(row['rating']) if row['rating'] and row['rating'] != 'None' else None
                        
                        # Only load cached entries that have ratings
                        if rating is not None:
                            self.csv_cache[letterboxd_url] = {
                                'title': row['title'],
                                'rating': rating,
                                'rating_count': row['rating_count'] if row['rating_count'] and row['rating_count'] != 'None' else None,
                                'year': row['year'] if row['year'] and row['year'] != 'None' else None,
                                'updated': row['updated'],
                                'url': letterboxd_url
                            }
                # print(f"Loaded {len(self.csv_cache)} cached ratings from {self.cache_file}")
            except Exception as e:
                print(f"Error loading cache: {e}")
                self.csv_cache = {}
    
    def _save_to_csv_cache(self, letterboxd_url: str, title: str, rating_data: Dict):
        """Save rating data to CSV cache"""
        self.csv_cache[letterboxd_url] = {
            'title': title,
            'rating': rating_data['rating'],
            'rating_count': rating_data['rating_count'],
            'year': rating_data['year'],
            'updated': datetime.now().isoformat(),
            'url': letterboxd_url
        }
        
        # Write to CSV file
        try:
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.exists(self.cache_file)
            
            with open(self.cache_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['letterboxd_url', 'title', 'rating', 'rating_count', 'year', 'updated']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow({
                    'letterboxd_url': letterboxd_url,
                    'title': title,
                    'rating': rating_data['rating'],
                    'rating_count': rating_data['rating_count'],
                    'year': rating_data['year'],
                    'updated': datetime.now().isoformat()
                })
        except Exception as e:
            print(f"Error saving to cache: {e}")
    
    def _get_from_cache(self, letterboxd_url: str) -> Optional[Dict]:
        """Get rating data from cache if updated within the last day"""
        if letterboxd_url in self.csv_cache:
            cached_data = self.csv_cache[letterboxd_url].copy()
            
            # Check if cached data is fresh (updated within last 24 hours)
            try:
                updated_time = datetime.fromisoformat(cached_data['updated'])
                time_diff = datetime.now() - updated_time
                
                if time_diff <= timedelta(days=1):
                    hours_ago = time_diff.total_seconds() // 3600
                    # print(f"Using cached rating for: {cached_data['title']} (cached {int(hours_ago)}h ago)")
                    return cached_data
                else:
                    print(f"Cache expired for: {cached_data['title']} (cached {time_diff.days} days ago), fetching fresh data")
                    return None
                    
            except (ValueError, KeyError) as e:
                print(f"Error parsing cache timestamp for {cached_data.get('title', 'unknown')}: {e}")
                return None
                
        return None

    def search_movie(self, title: str) -> Optional[str]:
        """Search for a movie and return its Letterboxd URL"""
        # Clean title for search
        clean_title = re.sub(r'[^\w\s]', '', title.lower())
        search_url = f"{self.base_url}/film/{clean_title.replace(' ', '+')}"
        
        try:
            response = requests.get(search_url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Find first film result
            film_link = soup.select_one('.film-detail a')
            if film_link:
                return self.base_url + film_link['href']
        except Exception as e:
            print(f"Error searching for {title}: {e}")
        
        return None
    
    def get_rating_from_url(self, letterboxd_url: str, title: str) -> Dict:
        """Get rating and metadata for a movie using direct Letterboxd URL"""
        # Check CSV cache first
        cached_data = self._get_from_cache(letterboxd_url)
        if cached_data:
            return cached_data
            
        if title in self.cache:
            return self.cache[title]
        
        # Rate limiting - commented out for maximum speed with threading
        # time.sleep(0.2)
        
        # Try the original URL first
        result = self._fetch_rating_from_url(letterboxd_url, title)
        if result['url'] is not None:  # Found the movie (even if no rating)
            return result
        
        # If failed and URL contains a year, try without the year
        if re.search(r'-\d{4}/?$', letterboxd_url):
            # Import the scraper to regenerate clean URL without year
            from .scraper import MovieScraper
            scraper = MovieScraper()
            # Remove year from title and regenerate URL
            title_without_year = re.sub(r'\s*\(\d{4}\)', '', title)
            # The generate_letterboxd_url expects just the clean title, not a title with year
            # So we pass the title without year, and it will generate a clean URL
            clean_url_without_year = scraper.generate_letterboxd_url(title_without_year)
            result_no_year = self._fetch_rating_from_url(clean_url_without_year, title)
            if result_no_year['url'] is not None:  # Found the movie (even if no rating)
                return result_no_year
            
            # Both attempts failed - try more fallbacks
            from .scraper import MovieScraper
            scraper = MovieScraper()
            
            # Try removing "with xxxxx" suffix
            if ' with ' in title.lower():
                title_without_with = re.sub(r'\s+with\s+.*$', '', title, flags=re.IGNORECASE)
                clean_url_without_with = scraper.generate_letterboxd_url(title_without_with)
                result_no_with = self._fetch_rating_from_url(clean_url_without_with, title)
                if result_no_with['url'] is not None:
                    return result_no_with
            
            # Try removing & completely (for cases like "Stiller & Meara" -> "Stiller Meara")
            if '&' in title:
                title_no_ampersand = re.sub(r'\s*&\s*', ' ', title)
                title_no_ampersand = re.sub(r'\s+', ' ', title_no_ampersand).strip()  # Clean up extra spaces
                clean_url_no_ampersand = scraper.generate_letterboxd_url(title_no_ampersand)
                result_no_ampersand = self._fetch_rating_from_url(clean_url_no_ampersand, title)
                if result_no_ampersand['url'] is not None:
                    return result_no_ampersand
            # else:
                # print(f"Movie not found at {letterboxd_url}, tried without year at {clean_url_without_year} - both failed")
                
            # All attempts failed, so truly not found
            return {
                'rating': None,
                'rating_count': None, 
                'url': None,  # Truly not found
                'year': None
            }
        
        # If URL doesn't have year and failed, try removing "with xxxxx" suffix
        if ' with ' in title.lower():
            # print(f"Movie not found at {letterboxd_url} - trying without 'with' suffix...")
            from .scraper import MovieScraper
            scraper = MovieScraper()
            # Remove "with xxxxx" suffix and regenerate URL
            title_without_with = re.sub(r'\s+with\s+.*$', '', title, flags=re.IGNORECASE)
            clean_url_without_with = scraper.generate_letterboxd_url(title_without_with)
            result_no_with = self._fetch_rating_from_url(clean_url_without_with, title)
            if result_no_with['url'] is not None:  # Found the movie (even if no rating)
                return result_no_with
        
        # Try removing & completely (for cases like "Stiller & Meara" -> "Stiller Meara")
        if '&' in title:
            # print(f"Movie not found at {letterboxd_url} - trying without ampersand...")
            from .scraper import MovieScraper
            scraper = MovieScraper()
            title_no_ampersand = re.sub(r'\s*&\s*', ' ', title)
            title_no_ampersand = re.sub(r'\s+', ' ', title_no_ampersand).strip()  # Clean up extra spaces
            clean_url_no_ampersand = scraper.generate_letterboxd_url(title_no_ampersand)
            result_no_ampersand = self._fetch_rating_from_url(clean_url_no_ampersand, title)
            if result_no_ampersand['url'] is not None:
                return result_no_ampersand
            
        # Truly not found
        return {
            'rating': None,
            'rating_count': None,
            'url': None,  # Truly not found
            'year': None
        }
    
    def _fetch_rating_from_url(self, letterboxd_url: str, title: str) -> Dict:
        """Internal method to fetch rating from a specific URL"""
       
        try:
            response = requests.get(letterboxd_url, headers=self.headers, timeout=10)
            if response.status_code != 200:
                return {'rating': None, 'rating_count': None, 'url': None, 'year': None}  # Not found
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Look for JSON-LD structured data
            json_scripts = soup.find_all('script', type='application/ld+json')
            rating = None
            rating_count = None
            year = None
            found_movie_data = False
            
            for script in json_scripts:
                try:
                    # Clean the script content - remove CDATA comments
                    content = script.string
                    if content:
                        # Remove CDATA wrapper
                        content = re.sub(r'/\*\s*<!\[CDATA\[\s*\*/\s*', '', content)
                        content = re.sub(r'\s*/\*\s*\]\]>\s*\*/', '', content)
                        content = content.strip()
                        
                        data = json.loads(content)
                        
                        # Check if we found movie data (even if no rating)
                        if isinstance(data, dict) and data.get('@type') == 'Movie':
                            found_movie_data = True
                            
                            # Extract year from dateCreated
                            if 'dateCreated' in data:
                                year_match = re.search(r'\d{4}', data['dateCreated'])
                                if year_match:
                                    year = year_match.group()
                            
                            if 'aggregateRating' in data:
                                aggregate = data['aggregateRating']
                                rating = float(aggregate.get('ratingValue', 0))
                                rating_count = int(aggregate.get('ratingCount', 0))
                            else:
                                # Movie found but no aggregateRating - try dynamic loading with Playwright
                                rating, rating_count, is_computed = asyncio.run(self._get_dynamic_rating(letterboxd_url))
                                if rating is not None and is_computed:
                                    # Mark this as computed from histogram
                                    rating_count = f"{rating_count}*"
                            break
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    continue
            
            # Fallback to HTML parsing if JSON-LD fails or if we found movie but no rating
            if rating is None and found_movie_data:
                rating_elem = soup.select_one('.average-rating')
                if rating_elem:
                    rating_text = rating_elem.text.strip()
                    rating = float(rating_text) if rating_text else None
                
                if rating is None:
                    # Movie found but no rating - add to special list
                    
                    self.movies_found_no_rating.append(letterboxd_url)
                
            elif rating is None:
                
                rating_elem = soup.select_one('.average-rating')
                if rating_elem:
                    rating_text = rating_elem.text.strip()
                    rating = float(rating_text) if rating_text else None
                    
                year_elem = soup.select_one('.film-title-wrapper a')
                if year_elem:
                    year_match = re.search(r'\d{4}', year_elem.text)
                    if year_match:
                        year = year_match.group()
                        
                # If we got here and found rating/year, that means the movie exists
                if rating is not None or year is not None:
                    found_movie_data = True
                    
            # Only set URL if we actually found the movie
            result = {
                'rating': rating,
                'rating_count': rating_count,
                'url': letterboxd_url if found_movie_data else None,
                'year': year,
                'computed_from_histogram': isinstance(rating_count, str) and rating_count.endswith('*')
            }
            
            # Only save to cache if we found the movie AND it has a rating
            # Movies without ratings should not be cached so they can be checked again
            if result['url'] is not None and result['rating'] is not None:
                self.cache[title] = result
                self._save_to_csv_cache(letterboxd_url, title, result)
            elif result['url'] is not None and result['rating'] is None:
                # Movie found but no rating - don't cache, add to special tracking list
                if letterboxd_url not in self.movies_found_no_rating:
                    self.movies_found_no_rating.append(letterboxd_url)
            
            
            return result
            
        except Exception as e:
            print(f"Error getting rating from URL {letterboxd_url}: {e}")
            return {'rating': None, 'rating_count': None, 'url': None, 'year': None}  # Error = not found

    def get_rating(self, title: str) -> Dict:
        """Get rating and metadata for a movie using search"""
        movie_url = self.search_movie(title)
        if not movie_url:
            return {'rating': None, 'rating_count': None, 'url': None, 'year': None}
        
        return self.get_rating_from_url(movie_url, title)
    
    async def _get_dynamic_rating(self, letterboxd_url: str) -> tuple:
        """Use Playwright to get rating from dynamically loaded content"""
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # Use shorter timeout and less strict wait condition
                await page.goto(letterboxd_url, wait_until='domcontentloaded', timeout=15000)
                
                # Wait for the CSI rating data to load
                try:
                    await page.wait_for_selector('.csi[data-on-load="rating-histogram"]', timeout=3000)
                    await page.wait_for_timeout(2000)  # Additional wait for content to populate
                except:
                    # Even if CSI doesn't load, try to get what we can
                    await page.wait_for_timeout(1000)  # Give it a moment
                
                # Print the HTML to see what we got
                html_content = await page.content()
                # print("=== DYNAMIC LOADING HTML ===")
                # print(html_content)
                # print("=== END DYNAMIC HTML ===")
                
                # Parse the HTML content with BeautifulSoup
                soup = BeautifulSoup(html_content, 'lxml')
                await browser.close()
                
                # Try to get rating from parsed HTML
                rating, rating_count, is_computed = self._parse_rating_from_html(soup)
                return rating, rating_count, is_computed
                    
        except Exception as e:
            print(f"Error with dynamic loading: {e}")
            return None, None, False
    
    def _parse_rating_from_html(self, soup: BeautifulSoup) -> tuple:
        """Parse rating from HTML soup - either from average-rating or histogram"""
        
        # First try to find an existing average rating
        avg_rating_elem = soup.select_one('.average-rating')
        if avg_rating_elem:
            try:
                rating = float(avg_rating_elem.text.strip())
                # Try to find rating count
                rating_count_elem = soup.select_one('.rating-count, [data-rating-count]')
                count = 0
                if rating_count_elem:
                    count_text = re.sub(r'[^0-9]', '', rating_count_elem.text)
                    count = int(count_text) if count_text else 0
                return rating, count, False
            except (ValueError, AttributeError):
                pass
        
        # Parse the rating histogram to calculate average ourselves
        histogram_bars = soup.select('.rating-histogram-bar a[data-original-title]')
        if histogram_bars:
            total_weighted_rating = 0
            total_count = 0
            
            # print(f"Found {len(histogram_bars)} histogram bars, parsing...")
            # print(histogram_bars)
            for bar in histogram_bars:
                tooltip = bar.get('data-original-title', '')
                # print(f"Parsing tooltip: {tooltip}")
                
                # Parse formats like "10 half-★ ratings (2%)" or "45 ★★ ratings (10%)"
                count_match = re.match(r'^(\d+)\s', tooltip)
                if count_match:
                    count = int(count_match.group(1))
                    
                    # Determine star value
                    star_value = 0
                    if 'half-★' in tooltip:
                        star_value = 0.5
                    else:
                        # Count ★ symbols
                        stars = tooltip.count('★')
                        if '½' in tooltip:
                            star_value = stars + 0.5
                        else:
                            star_value = stars
                    
                    # print(f"  {count} ratings at {star_value} stars")
                    total_weighted_rating += count * star_value
                    total_count += count
            
            if total_count > 0:
                average_rating = total_weighted_rating / total_count
                average_rating = round(average_rating, 2)  # Round to 2 decimals
                # print(f"Calculated average: {average_rating} from {total_count} total ratings")
                return average_rating, total_count, True  # True = computed from histogram
        
        return None, None, False
    
    def filter_movies_by_cache(self, movies: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """Separate movies into cached and uncached lists"""
        cached_movies = []
        uncached_movies = []
        
        for movie in movies:
            letterboxd_url = movie.get('letterboxd_url')
            if letterboxd_url and letterboxd_url in self.csv_cache:
                # Movie is in cache, add cached data
                cached_data = self.csv_cache[letterboxd_url]
                movie['letterboxd_rating'] = cached_data['rating']
                movie['letterboxd_url'] = cached_data['url']
                movie['year'] = cached_data['year']
                cached_movies.append(movie)
            else:
                # Movie needs to be processed
                uncached_movies.append(movie)
        
        return cached_movies, uncached_movies
    
    def process_movie_batch(self, movies: List[Dict], progress_callback=None, max_workers=12) -> List[Dict]:
        """Process multiple movies concurrently with threading"""
        if not movies:
            return []
        
        # Filter movies by cache first
        cached_movies, uncached_movies = self.filter_movies_by_cache(movies)
        
        if progress_callback:
            progress_callback(f"📂 Found {len(cached_movies)} movies in cache, processing {len(uncached_movies)} new movies")
        
        if not uncached_movies:
            return cached_movies
        
        # Process uncached movies with threading
        processed_movies = []
        movies_not_found = []
        
        def process_single_movie(movie):
            """Process a single movie - thread-safe"""
            letterboxd_url = movie.get('letterboxd_url')
            title = movie.get('title', 'Unknown')
            
            if letterboxd_url:
                rating_data = self.get_rating_from_url(letterboxd_url, title)
                
                # Thread-safe update of movie data
                movie['letterboxd_rating'] = rating_data['rating']
                movie['letterboxd_url'] = rating_data['url']
                movie['year'] = rating_data['year']
                
                if rating_data['rating'] is None and rating_data['url'] is None:
                    with self._lock:
                        movies_not_found.append(movie)
            
            return movie
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_movie = {executor.submit(process_single_movie, movie): movie for movie in uncached_movies}
            
            # Process completed jobs
            completed = 0
            for future in as_completed(future_to_movie):
                try:
                    processed_movie = future.result()
                    processed_movies.append(processed_movie)
                    completed += 1
                    
                    if progress_callback and completed % max(1, len(uncached_movies) // 10) == 0:
                        progress_callback(f"📊 Processed {completed}/{len(uncached_movies)} movies ({completed/len(uncached_movies)*100:.0f}%)")
                        
                except Exception as e:
                    movie = future_to_movie[future]
                    if progress_callback:
                        progress_callback(f"❌ Error processing {movie.get('title', 'Unknown')}: {e}")
        
        # Update the movies_not_found list in a thread-safe way
        with self._lock:
            self.movies_found_no_rating.extend([url for movie in movies_not_found for url in [movie.get('letterboxd_url')] if url])
        
        # Combine cached and processed movies
        all_movies = cached_movies + processed_movies
        
        if progress_callback:
            progress_callback(f"✅ Completed processing {len(all_movies)} total movies ({len(cached_movies)} from cache, {len(processed_movies)} newly processed)")
        
        return all_movies
