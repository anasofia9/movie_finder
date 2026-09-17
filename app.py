from flask import Flask, render_template, jsonify, request, Response
from datetime import datetime
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo('America/New_York')

def eastern_now():
    """Naive Eastern-time now (the server runs in UTC on Render)."""
    return datetime.now(EASTERN).replace(tzinfo=None)
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
    'rating_threshold': 4.0,
    'data_notice': None
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
        movies_data['data_notice'] = None

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
        movies = letterboxd.process_movie_batch(movies, progress_callback=log_status, max_workers=8)
        
        # Get movies that weren't found
        movies_not_found = [movie for movie in movies if movie.get('letterboxd_rating') is None and movie.get('letterboxd_url') is None]
        
        log_status(f"⭐ Found ratings for {len([m for m in movies if m.get('letterboxd_rating')])} movies")
        log_status(f"⚠️  {len(letterboxd.movies_found_no_rating)} movies found on Letterboxd but no ratings yet")
        log_status(f"❌ {len(movies_not_found)} movies not found on Letterboxd")
        
        # Update global data
        movies_data['movies'] = movies
        movies_data['movies_not_found'] = movies_not_found
        movies_data['movies_found_no_rating'] = letterboxd.movies_found_no_rating
        movies_data['last_updated'] = eastern_now()
        movies_data['is_scraping'] = False
        
        log_status("✅ Scraping completed successfully!")
        
    except Exception as e:
        error_msg = f"💥 Error during scraping: {e}"
        log_status(error_msg)
        movies_data['is_scraping'] = False

def load_cached_data():
    """Hydrate movies_data from caches only (Supabase or local files).
    Never scrapes — if today's cache is empty/stale, leaves the app in the
    'no data' state so the user can trigger a refresh."""
    global movies_data
    try:
        scraper = MovieScraper(log_callback=log_status)
        movies, newest_cached_at = scraper.get_cached_movies_only()
        if not movies:
            if newest_cached_at:
                movies_data['data_notice'] = ("⚠️ All cached data is more than a week old. "
                                              "Click Refresh Data to scrape fresh listings.")
                log_status("📭 Cached data is more than a week old — click Refresh Data to scrape.")
            else:
                log_status("📭 No cached data yet — click Refresh Data to scrape.")
            return

        letterboxd = LetterboxdAPI()
        with_data, without_data = letterboxd.apply_cached_ratings(movies)
        movies = with_data + without_data

        # Don't clobber a scrape the user kicked off in the meantime
        if movies_data['is_scraping'] or movies_data['movies']:
            return

        movies_data['movies'] = movies
        movies_data['movies_not_found'] = [
            m for m in movies
            if m.get('letterboxd_rating') is None and m.get('letterboxd_url') is None
        ]
        movies_data['movies_found_no_rating'] = []

        last_updated = eastern_now()
        if newest_cached_at:
            try:
                last_updated = datetime.fromisoformat(str(newest_cached_at)).astimezone(EASTERN).replace(tzinfo=None)
            except ValueError:
                pass
        movies_data['last_updated'] = last_updated

        rated = len([m for m in with_data if m.get('letterboxd_rating')])
        log_status(f"📦 Loaded {len(movies)} movies from today's cache ({rated} with ratings)")
    except Exception as e:
        log_status(f"⚠️ Could not load cached data: {e}")


# Hydrate from caches on startup (import time, so it also runs under
# gunicorn on Render). Cache-only: never triggers a scrape.
threading.Thread(target=load_cached_data, daemon=True).start()


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
    
    # All genres saved in the ratings DB (Supabase or local CSV) for the filter checkboxes
    try:
        all_genres = LetterboxdAPI().get_all_genres()
    except Exception:
        all_genres = []

    return render_template('index.html',
                         movies=movies_data['movies'],
                         last_updated=movies_data['last_updated'],
                         is_scraping=movies_data['is_scraping'],
                         rating_threshold=movies_data['rating_threshold'],
                         status_messages=status_messages[-10:],
                         all_genres=all_genres,
                         data_notice=movies_data['data_notice'],
                         newsletter_content=newsletter_content)

@app.route('/newsletter')
def newsletter():
    """Standalone view of the generated weekly newsletter."""
    if not movies_data['movies']:
        return Response("<p style='font-family: sans-serif; padding: 40px'>No data yet — refresh the movie listings first.</p>", mimetype='text/html')
    generator = NewsletterGenerator(rating_threshold=movies_data['rating_threshold'])
    content = generator.generate_html(
        movies_data['movies'],
        movies_data['movies_not_found'],
        movies_data['movies_found_no_rating']
    )
    return Response(content, mimetype='text/html')


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


# Memoized cache status (avoids a Supabase query on every 5s status poll)
_cache_status_memo = {'time': 0, 'value': None}

def get_cache_status_memoized(max_age_seconds=60):
    now = time.time()
    if _cache_status_memo['value'] is None or now - _cache_status_memo['time'] > max_age_seconds:
        scraper = MovieScraper(log_callback=lambda msg: None)
        _cache_status_memo['value'] = scraper.get_cache_status()
        _cache_status_memo['time'] = now
    return _cache_status_memo['value']

@app.route('/api/status')
def api_status():
    """Get current scraping status"""
    cache_status = get_cache_status_memoized()
    
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
    app.run(host='0.0.0.0', port=8000, debug=True)