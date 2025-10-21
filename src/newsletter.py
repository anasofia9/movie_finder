from datetime import datetime
from typing import List, Dict
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

class NewsletterGenerator:
    """Generate and send newsletter"""
    
    def __init__(self):
        self.sendgrid_key = os.getenv('SENDGRID_API_KEY')
        self.recipient = os.getenv('RECIPIENT_EMAIL')
    
    def generate_html(self, movies: List[Dict]) -> str:
        """Generate HTML newsletter"""
        today = datetime.now().strftime('%B %d, %Y')
        
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
            <p>Here are this week's top-rated movies playing in NYC theaters:</p>
        """
        
        if not movies:
            html += "<p>No movies found this week.</p>"
        else:
            for i, movie in enumerate(movies, 1):
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
