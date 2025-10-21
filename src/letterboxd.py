import requests
from bs4 import BeautifulSoup
import re
import time
import json
from typing import Optional, Dict

class LetterboxdAPI:
    """Fetch Letterboxd ratings for movies"""
    
    def __init__(self):
        self.base_url = "https://letterboxd.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.cache = {}
    
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
        if title in self.cache:
            return self.cache[title]
        
        # Rate limiting
        time.sleep(1)
        
        try:
            response = requests.get(letterboxd_url, headers=self.headers, timeout=10)
            if response.status_code != 200:
                print(f"Movie not found at {letterboxd_url}, falling back to search")
                return self.get_rating(title)
            
            soup = BeautifulSoup(response.content, 'lxml')
            # print(soup)
            # print("--------------------------------------------------------")
            # Look for JSON-LD structured data
            json_scripts = soup.find_all('script', type='application/ld+json')
            # print(json_scripts)
            rating = None
            rating_count = None
            year = None
            
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
                        
                        if isinstance(data, dict) and 'aggregateRating' in data:
                            aggregate = data['aggregateRating']
                            rating = float(aggregate.get('ratingValue', 0))
                            rating_count = int(aggregate.get('ratingCount', 0))
                            
                            
                            # Extract year from dateCreated
                            if 'dateCreated' in data:
                                year_match = re.search(r'\d{4}', data['dateCreated'])
                                if year_match:
                                    year = year_match.group()
                            break
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    print(f"Error parsing JSON: {e}")
                    continue
            
            # Fallback to HTML parsing if JSON-LD fails
            if rating is None:
                rating_elem = soup.select_one('.average-rating')
                if rating_elem:
                    rating_text = rating_elem.text.strip()
                    rating = float(rating_text) if rating_text else None
                
                year_elem = soup.select_one('.film-title-wrapper a')
                if year_elem:
                    year_match = re.search(r'\d{4}', year_elem.text)
                    if year_match:
                        year = year_match.group()
            
            result = {
                'rating': rating,
                'rating_count': rating_count,
                'url': letterboxd_url,
                'year': year
            }
            
            self.cache[title] = result
            return result
            
        except Exception as e:
            print(f"Error getting rating from URL {letterboxd_url}: {e}")
            return {'rating': None, 'rating_count': None, 'url': None, 'year': None}

    def get_rating(self, title: str) -> Dict:
        """Get rating and metadata for a movie using search"""
        movie_url = self.search_movie(title)
        if not movie_url:
            return {'rating': None, 'rating_count': None, 'url': None, 'year': None}
        
        return self.get_rating_from_url(movie_url, title)
