from datetime import datetime
from typing import List, Dict
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

class NewsletterGenerator:
    """Generate and send newsletter"""
    
    def __init__(self, rating_threshold: float = 4.0):
        self.sendgrid_key = os.getenv('SENDGRID_API_KEY')
        self.recipient = os.getenv('RECIPIENT_EMAIL')
        self.rating_threshold = rating_threshold
    
    def generate_html(self, movies: List[Dict], movies_not_found: List[Dict] = None, movies_found_no_rating: List[str] = None) -> str:
        """Generate HTML newsletter with rating threshold filtering"""
        today = datetime.now().strftime('%B %d, %Y')
        
        # Filter movies by rating threshold and sort by rating (descending)
        high_rated_movies = [m for m in movies if m.get('letterboxd_rating') and m.get('letterboxd_rating') >= self.rating_threshold]
        high_rated_movies.sort(key=lambda x: x.get('letterboxd_rating', 0), reverse=True)
        
       
        for movie in high_rated_movies:
            print(f"  - {movie.get('title')}: {movie.get('letterboxd_rating')}")
        
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; }}
                h1 {{ color: #2c3e50; }}
                .movie {{ border-bottom: 1px solid #eee; padding: 15px 0; }}
                .movie-title {{ font-size: 18px; font-weight: bold; color: #34495e; }}
                .rating {{ color: #27ae60; font-weight: bold; font-size: 16px; }}
                .venue {{ color: #7f8c8d; font-size: 14px; }}
                .high-rating {{ background-color: #d5f4e6; padding: 2px 8px; border-radius: 3px; }}
                a {{ color: #3498db; text-decoration: none; }}
            </style>
        </head>
        <body>
            <h1>🎬 NYC Movie Picks - {today}</h1>
            <p>Here are this week's top-rated movies (≥{self.rating_threshold}⭐) playing in NYC theaters:</p>
        """
        
        if not high_rated_movies:
            html += f"<p>No movies found with rating >= {self.rating_threshold} this week.</p>"
        else:
            for i, movie in enumerate(high_rated_movies, 1):
                rating = movie.get('letterboxd_rating')
                rating_display = f"⭐ {rating:.1f}" if rating else "N/A"
                rating_class = "high-rating" if rating and rating >= 4.0 else ""
                
                html += f"""
                <div class="movie">
                    <div class="movie-title">{i}. {movie['title']}</div>
                    <div class="rating {rating_class}">{rating_display}</div>
                    <div class="venue">📍 {movie['venue']}</div>
                    <div>
                        <a href="{movie.get('letterboxd_url', '#')}">Letterboxd</a>
                        {f" | <a href='{movie['url']}'>Tickets</a>" if movie.get('url') else ""}
                    </div>
                </div>
                """
        
        # Add section for movies found but with no ratings
        if movies_found_no_rating:
            html += f"""
            <h2 style="color: #f39c12; margin-top: 30px;">⚠️ Movies Found on Letterboxd (No Ratings Yet)</h2>
            <p style="color: #7f8c8d;">These movies are on Letterboxd but don't have ratings yet:</p>
            """
            
            # Get movie titles from the movies list that correspond to the URLs
            for i, url in enumerate(movies_found_no_rating, 1):
                # Find the movie with this URL
                movie_title = "Unknown Title"
                movie_venue = "Unknown Venue"
                for movie in movies:
                    if movie.get('letterboxd_url') == url:
                        movie_title = movie['title']
                        movie_venue = movie.get('venue', 'Unknown Venue')
                        break
                
                html += f"""
                <div class="movie" style="opacity: 0.7;">
                    <div class="movie-title">{i}. {movie_title}</div>
                    <div style="color: #f39c12; font-weight: bold;">⚠️ No rating yet</div>
                    <div class="venue">📍 {movie_venue}</div>
                    <div>
                        <a href="{url}">Letterboxd</a>
                    </div>
                </div>
                """
        
        # Add section for movies not found on Letterboxd
        if movies_not_found:
            html += f"""
            <h2 style="color: #e74c3c; margin-top: 30px;">❌ Screenings Not Found on Letterboxd</h2>
            <p style="color: #7f8c8d;">These movies could not be found on Letterboxd:</p>
            """
            
            for i, movie in enumerate(movies_not_found, 1):
                sources = movie.get('sources', [movie.get('source', 'unknown')])
                sources_str = ', '.join(sources)
                
                html += f"""
                <div class="movie" style="opacity: 0.6;">
                    <div class="movie-title">{i}. {movie['title']}</div>
                    <div style="color: #e74c3c; font-weight: bold;">❌ Not found</div>
                    <div class="venue">📍 {movie.get('venue', 'Unknown Venue')} | Sources: {sources_str}</div>
                    <div>
                        {f"<a href='{movie['url']}'>Tickets</a>" if movie.get('url') else "No ticket link available"}
                    </div>
                </div>
                """
        
        html += """
        </body>
        </html>
        """
        
        return html
    
    def save_to_file(self, content: str):
        """Save newsletter to file"""
        os.makedirs('newsletters', exist_ok=True)
        date_str = datetime.now().strftime('%Y-%m-%d')
        filename = f"newsletters/newsletter-{date_str}.html"
        
        with open(filename, 'w') as f:
            f.write(content)
        
        print(f"Newsletter saved to {filename}")
    
    def send_email(self, html_content: str):
        """Send newsletter via SendGrid"""
        if not self.sendgrid_key or not self.recipient:
            print("SendGrid credentials not configured")
            return
        
        try:
            message = Mail(
                from_email='your-email@example.com',  # Configure this
                to_emails=self.recipient,
                subject=f'NYC Movie Picks - {datetime.now().strftime("%B %d")}',
                html_content=html_content
            )
            
            sg = SendGridAPIClient(self.sendgrid_key)
            response = sg.send(message)
            print(f"Email sent! Status code: {response.status_code}")
            
        except Exception as e:
            print(f"Error sending email: {e}")
