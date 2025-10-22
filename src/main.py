from scraper import MovieScraper
from letterboxd import LetterboxdAPI
from newsletter import NewsletterGenerator

def main():
    print("🎬 Starting Movie Finder...")
    
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
        # print(movie)
        # Use direct URL if available (from Alamo scraper), otherwise search
        if 'letterboxd_url' in movie and movie['letterboxd_url']:
            rating_data = letterboxd.get_rating_from_url(movie['letterboxd_url'], movie['title'])
        # else:
        #     rating_data = letterboxd.get_rating(movie['title'])
        
        movie['letterboxd_rating'] = rating_data['rating']
        movie['letterboxd_url'] = rating_data['url']
        movie['year'] = rating_data['year']
        
        if rating_data['rating'] is None:
            movies_not_found.append(movie)
        else:
            print(f"  {movie['title']}: {rating_data['rating']}")
    # print("---------- ITEMS NOT FOUND ON LETTERBOXD ----------")
    # for movie in movies_not_found:
    #     print(f" title = {movie['title']}, url = {movie['letterboxd_url']}")
    
    # Filter and sort
    print("\n🔍 Filtering and sorting...")
    # Only include movies with ratings
    rated_movies = [m for m in movies if m.get('letterboxd_rating')]
    # Sort by rating (descending)
    rated_movies.sort(key=lambda x: x.get('letterboxd_rating', 0), reverse=True)
    # Take top 15
    top_movies = rated_movies[:15]
    
    print(f"Top {len(top_movies)} movies selected")
    
    # Generate newsletter
    print("\n📰 Generating newsletter...")
    generator = NewsletterGenerator()
    html_content = generator.generate_html(top_movies)
    
    # Save to file
    generator.save_to_file(html_content)
    
    # Send email
    generator.send_email(html_content)
    
    # Print movies not found on Letterboxd
    if movies_not_found:
        print(f"\n❌ Movies not found on Letterboxd ({len(movies_not_found)}):")
        for movie in movies_not_found:
            print(f" title = {movie['title']}, url = {movie['letterboxd_url']}")
    else:
        print("\n✅ All movies found on Letterboxd!")
    
    print("\n✅ Done!")

if __name__ == "__main__":
    main()
