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
    

    # Metallic tones matching the main site's card headers: (bg, accent)
    METALS = [
        ('#262013', '#d4a94e'),  # brass
        ('#271b12', '#c98a63'),  # copper
        ('#161e27', '#8badc9'),  # steel
        ('#142420', '#74b295'),  # verdigris
        ('#201f26', '#b2b2c4'),  # pewter
        ('#271a1d', '#cf9288'),  # rose gold
        ('#181d20', '#93a4ad'),  # gunmetal
        ('#231c10', '#bd9a60'),  # bronze
    ]

    def _movie_row(self, i, title, badge, badge_color, venue, dates, links_html, dim=False):
        bg, accent = self.METALS[i % len(self.METALS)]
        opacity = 'opacity: 0.75;' if dim else ''
        dates_html = f"<div style='color: #97907f; font-size: 12.5px; margin-top: 6px;'>Showing {dates}</div>" if dates else ''
        return f"""
        <div style="background: #161511; border: 1px solid #292620; border-radius: 12px; margin-bottom: 14px; overflow: hidden; {opacity}">
            <div style="background: {bg}; padding: 14px 18px 15px; border-bottom: 1px solid #292620;">
                <table width="100%" cellpadding="0" cellspacing="0"><tr>
                    <td><span style="display: inline-block; border: 1px solid rgba(212,169,78,0.35); border-radius: 6px; background: rgba(0,0,0,0.3); padding: 2px 9px; font-size: 12.5px; font-weight: 600; color: {badge_color};">{badge}</span></td>
                    <td align="right" style="font-size: 9.5px; letter-spacing: 1.5px; text-transform: uppercase; color: #6e6858;">Letterboxd</td>
                </tr></table>
                <div style="font-family: 'Poiret One', sans-serif; font-size: 23px; text-transform: uppercase; letter-spacing: 0.5px; color: #ece7dc; margin-top: 10px; line-height: 1.1;">{title}</div>
                <div style="width: 40px; height: 2px; background: {accent}; margin-top: 10px;"></div>
            </div>
            <div style="padding: 12px 18px 14px;">
                <div style="font-size: 13.5px; font-weight: 500; color: #ece7dc;"><span style="display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: {accent}; margin-right: 8px; vertical-align: 1px;"></span>{venue}</div>
                {dates_html}
                <div style="margin-top: 10px; font-size: 12.5px;">{links_html}</div>
            </div>
        </div>
        """

    def generate_html(self, movies: List[Dict], movies_not_found: List[Dict] = None, movies_found_no_rating: List[str] = None) -> str:
        """Generate the weekly journal HTML, styled to match the main site."""
        today = datetime.now().strftime('%B %d, %Y')

        high_rated_movies = [m for m in movies if m.get('letterboxd_rating') and m.get('letterboxd_rating') >= self.rating_threshold]
        high_rated_movies.sort(key=lambda x: x.get('letterboxd_rating', 0), reverse=True)

        lb_link = "color: #d4a94e; font-weight: 600; text-decoration: none;"
        mut_link = "color: #97907f; font-weight: 600; text-decoration: none;"

        html = f"""<!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Weekly Cinephile Journal — {today}</title>
            <link href="https://fonts.googleapis.com/css2?family=Poiret+One&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
        </head>
        <body style="margin: 0; padding: 36px 20px; background-color: #0d0c0a; font-family: 'Inter', -apple-system, sans-serif; color: #ece7dc;">
        <div style="max-width: 640px; margin: 0 auto;">
            <div style="font-size: 10px; letter-spacing: 1.5px; text-transform: uppercase; color: #d4a94e; margin-bottom: 10px;">Cinephile Editions &middot; {today}</div>
            <h1 style="font-family: 'Poiret One', sans-serif; font-weight: 400; font-size: 40px; text-transform: uppercase; letter-spacing: 2px; margin: 0 0 10px; color: #ece7dc;">Weekly Cinephile Journal</h1>
            <p style="margin: 0 0 26px; color: #97907f; font-size: 14.5px;">The top-rated films (&ge;{self.rating_threshold}&#9733; on Letterboxd) now playing or coming soon at NYC's independent theaters.</p>
            <hr style="border: none; border-top: 1px solid #292620; margin: 0 0 26px;">
        """

        if not high_rated_movies:
            html += f"<p style='color: #6e6858;'>No movies rated {self.rating_threshold}&#9733; or higher right now.</p>"
        else:
            for i, movie in enumerate(high_rated_movies):
                rating = movie.get('letterboxd_rating')
                links = []
                if movie.get('letterboxd_url'):
                    links.append(f"<a href='{movie['letterboxd_url']}' style='{lb_link}'>Letterboxd Review &#8599;</a>")
                if movie.get('url'):
                    links.append(f"<a href='{movie['url']}' style='{mut_link}'>Venue Tickets &rsaquo;</a>")
                html += self._movie_row(
                    i, movie['title'], f"{rating:.1f} &#9733;", '#d4a94e',
                    movie.get('venue', ''), movie.get('dates_display') or '',
                    ' &nbsp;&middot;&nbsp; '.join(links)
                )

        if movies_found_no_rating:
            html += """
            <h2 style="font-family: 'Poiret One', sans-serif; font-weight: 400; font-size: 24px; text-transform: uppercase; letter-spacing: 1px; margin: 34px 0 6px;">Awaiting ratings</h2>
            <p style="margin: 0 0 16px; color: #6e6858; font-size: 12.5px;">On Letterboxd, not enough audience scores yet.</p>
            """
            for i, url in enumerate(movies_found_no_rating):
                movie_title, movie_venue, dates = 'Unknown Title', '', ''
                for movie in movies:
                    if movie.get('letterboxd_url') == url:
                        movie_title = movie['title']
                        movie_venue = movie.get('venue', '')
                        dates = movie.get('dates_display') or ''
                        break
                html += self._movie_row(
                    4, movie_title, 'Unrated', '#97907f', movie_venue, dates,
                    f"<a href='{url}' style='{lb_link}'>Letterboxd &#8599;</a>", dim=True
                )

        if movies_not_found:
            html += """
            <h2 style="font-family: 'Poiret One', sans-serif; font-weight: 400; font-size: 24px; text-transform: uppercase; letter-spacing: 1px; margin: 34px 0 6px;">Off the record</h2>
            <p style="margin: 0 0 16px; color: #6e6858; font-size: 12.5px;">Screenings we couldn't match on Letterboxd.</p>
            """
            for movie in movies_not_found:
                link = f"<a href='{movie['url']}' style='{mut_link}'>Venue Tickets &rsaquo;</a>" if movie.get('url') else "<span style='color: #6e6858;'>No ticket link</span>"
                html += self._movie_row(
                    6, movie['title'], 'Not matched', '#97907f',
                    movie.get('venue', ''), movie.get('dates_display') or '', link, dim=True
                )

        html += """
            <hr style="border: none; border-top: 1px solid #292620; margin: 30px 0 18px;">
            <p style="font-size: 11.5px; color: #6e6858;">&copy; 2026 NYC Movie Finder. Not affiliated with Letterboxd or its associated platforms.</p>
        </div>
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
