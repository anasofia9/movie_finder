from flask import Flask, render_template, jsonify, request, Response
from datetime import datetime
import threading
import time
import json
import queue
from src.scraper import MovieScraper
from src.letterboxd import LetterboxdAPI
from src.newsletter import NewsletterGenerator

app = Flask(__name__)

# Global variables to store data
movies_data = {
    'movies': [],
    'movies_not_found': [],
    'movies_found_no_rating': [],
    'last_updated': None,
    'is_scraping': False,
    'rating_threshold': 4.0
}

# Status logging system
status_queue = queue.Queue()
status_messages = []

def log_status(message):
    """Log a status message for display on the web interface"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    status_entry = f"[{timestamp}] {message}"
    print(status_entry)  # Still print to console
    status_messages.append(status_entry)
    status_queue.put(status_entry)
    # Keep only last 50 messages to prevent memory issues
    if len(status_messages) > 50:
        status_messages.pop(0)

def scrape_movies(selected_theaters=None, disable_cache=False):
    """Background task to scrape movies"""
    global movies_data
    
    try:
        movies_data['is_scraping'] = True
        # Clear existing data immediately when starting new scrape
        movies_data['movies'] = []
        movies_data['movies_not_found'] = []
        movies_data['movies_found_no_rating'] = []
        movies_data['last_updated'] = None
        
        status_messages.clear()  # Clear previous messages
        log_status("🎬 Starting Movie Scraping...")
        
        # Scrape movie listings
        scraper = MovieScraper(log_callback=log_status, use_cache=not disable_cache)
        if selected_theaters:
            theater_names = [t.replace('_', ' ').title() for t in selected_theaters]
            log_status(f"📍 Scraping selected theaters: {', '.join(theater_names)}")
        else:
            log_status("📍 Scraping all movie theaters...")
        movies = scraper.get_all_movies(selected_theaters)
        log_status(f"✅ Found {len(movies)} movies from selected theaters")
        
        # Get Letterboxd ratings using batch processing
        log_status("🔍 Looking up Letterboxd ratings...")
        letterboxd = LetterboxdAPI()
        
        # Process all movies at once with caching and multithreading
        movies = letterboxd.process_movie_batch(movies, progress_callback=log_status, max_workers=15)
        
        # Get movies that weren't found
        movies_not_found = [movie for movie in movies if movie.get('letterboxd_rating') is None and movie.get('letterboxd_url') is None]
        
        log_status(f"⭐ Found ratings for {len([m for m in movies if m.get('letterboxd_rating')])} movies")
        log_status(f"⚠️  {len(letterboxd.movies_found_no_rating)} movies found on Letterboxd but no ratings yet")
        log_status(f"❌ {len(movies_not_found)} movies not found on Letterboxd")
        
        # Update global data
        movies_data['movies'] = movies
        movies_data['movies_not_found'] = movies_not_found
        movies_data['movies_found_no_rating'] = letterboxd.movies_found_no_rating
        movies_data['last_updated'] = datetime.now()
        movies_data['is_scraping'] = False
        
        log_status("✅ Scraping completed successfully!")
        
    except Exception as e:
        error_msg = f"💥 Error during scraping: {e}"
        log_status(error_msg)
        movies_data['is_scraping'] = False

@app.route('/')
def index():
    """Main page showing movie listings"""
    # Generate newsletter content if we have movies
    newsletter_content = ""
    if movies_data['movies']:
        generator = NewsletterGenerator(rating_threshold=movies_data['rating_threshold'])
        newsletter_content = generator.generate_html(
            movies_data['movies'], 
            movies_data['movies_not_found'], 
            movies_data['movies_found_no_rating']
        )
    
    return render_template('index.html', 
                         movies=movies_data['movies'],
                         last_updated=movies_data['last_updated'],
                         is_scraping=movies_data['is_scraping'],
                         rating_threshold=movies_data['rating_threshold'],
                         status_messages=status_messages[-10:],
                         newsletter_content=newsletter_content)

@app.route('/api/movies')
def api_movies():
    """API endpoint to get all movies data"""
    return jsonify(movies_data)

@app.route('/api/refresh', methods=['GET', 'POST'])
def api_refresh():
    """API endpoint to trigger a refresh"""
    if not movies_data['is_scraping']:
        selected_theaters = None
        rating_threshold = 4.0
        
        disable_cache = False
        
        if request.method == 'POST' and request.is_json:
            data = request.get_json()
            selected_theaters = data.get('theaters')
            disable_cache = data.get('disable_cache', False)
            if 'rating_threshold' in data:
                try:
                    rating_threshold = float(data['rating_threshold'])
                    movies_data['rating_threshold'] = rating_threshold
                except (ValueError, TypeError):
                    rating_threshold = 4.0
        
        thread = threading.Thread(target=scrape_movies, args=(selected_theaters, disable_cache))
        thread.daemon = True
        thread.start()
        return jsonify({'status': 'started', 'message': 'Scraping started'})
    else:
        return jsonify({'status': 'already_running', 'message': 'Scraping already in progress'})


@app.route('/api/status')
def api_status():
    """Get current scraping status"""
    # Get cache status
    scraper = MovieScraper()
    cache_status = scraper.get_cache_status()
    
    return jsonify({
        'is_scraping': movies_data['is_scraping'],
        'last_updated': movies_data['last_updated'].isoformat() if movies_data['last_updated'] else None,
        'total_movies': len(movies_data['movies']),
        'movies_not_found': len(movies_data['movies_not_found']),
        'movies_no_rating': len(movies_data['movies_found_no_rating']),
        'status_messages': status_messages[-10:],  # Last 10 messages
        'cache_status': cache_status
    })

if __name__ == '__main__':
    # Initial scrape on startup (in background) - scrape all theaters by default
    thread = threading.Thread(target=scrape_movies, args=(None,))
    thread.daemon = True
    thread.start()
    
    app.run(host='0.0.0.0', port=8000, debug=True)