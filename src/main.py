from scraper import MovieScraper
from letterboxd import LetterboxdAPI
from newsletter import NewsletterGenerator

def main():
    print("🎬 Starting Movie Finder...")
    
    # Configuration
    RATING_THRESHOLD = 4.0  # Only include movies with rating >= this value
    
    # Scrape movie listings
    print("\n📡 Scraping movie listings...")
    scraper = MovieScraper()
    movies = scraper.get_all_movies()
    print(f"Found {len(movies)} movies")
    
    # Get Letterboxd ratings
    print("\n⭐ Fetching Letterboxd ratings...")
    letterboxd = LetterboxdAPI()
    movies_not_found = []
    
    for movie in movies:
        
        # Use direct URL if available (from Alamo scraper), otherwise search
        if 'letterboxd_url' in movie and movie['letterboxd_url']:
            rating_data = letterboxd.get_rating_from_url(movie['letterboxd_url'], movie['title'])
        # else:
        #     rating_data = letterboxd.get_rating(movie['title'])
            
        
        movie['letterboxd_rating'] = rating_data['rating']
        movie['letterboxd_url'] = rating_data['url']
        movie['year'] = rating_data['year']
        
       
        if rating_data['rating'] is None:
            # Only add to movies_not_found if the URL is None (truly not found)
            # If URL exists but no rating, it's already in letterboxd.movies_found_no_rating
            if rating_data['url'] is None:
                movies_not_found.append(movie)
        else:
            rating_display = rating_data['rating']
            if rating_data.get('computed_from_histogram', False):
                rating_display = f"{rating_display} (computed)"
            print(f"  {movie['title']}: {rating_display}")
    # print("---------- ITEMS NOT FOUND ON LETTERBOXD ----------")
    # for movie in movies_not_found:
    #     print(f" title = {movie['title']}, url = {movie['letterboxd_url']}")
    
    
    # Generate newsletter
    print("\n📰 Generating newsletter...")
    generator = NewsletterGenerator(rating_threshold=RATING_THRESHOLD)
    html_content = generator.generate_html(movies, movies_not_found, letterboxd.movies_found_no_rating)
    
    # Save to file
    generator.save_to_file(html_content)
    
    # Send email
    generator.send_email(html_content)
    
    # Print movies not found on Letterboxd
    if movies_not_found:
        print(f"\n❌ Movies not found on Letterboxd ({len(movies_not_found)}):")
        for movie in movies_not_found:
            sources = movie.get('sources', [movie.get('source', 'unknown')])
            sources_str = ', '.join(sources)
            print(f" [{sources_str}] {movie['title']}")
    else:
        print("\n✅ All movies found on Letterboxd!")
    
    # Print movies found but without ratings
    if letterboxd.movies_found_no_rating:
        print(f"\n⚠️ Movies found on Letterboxd but no ratings yet ({len(letterboxd.movies_found_no_rating)}):")
        for url in letterboxd.movies_found_no_rating:
            print(f" {url}")
    
    print("\n✅ Done!")

if __name__ == "__main__":
    main()
