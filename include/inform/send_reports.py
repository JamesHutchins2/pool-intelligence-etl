"""
Inform module for listings ETL pipeline.

This module handles:
1. Fetching licenses from master database (each with admin_email and bounds polygon)
2. For each license, querying new/removed listings within bounds from past week
3. Creating XLSX files for new and removed listings
4. Sending email with attachments via Resend API
"""

import os
import base64
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import List, Dict, Any

import pandas as pd
import requests
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
MASTER_DB_URL = os.getenv("MASTER_DB_URL")


def get_all_licenses(conn) -> List[Dict[str, Any]]:

    query = """
        SELECT 
            id,
            admin_email,
            ST_AsText(bounds) as bounds_wkt,
            type,
            service_area
        FROM licenses
        WHERE bounds IS NOT NULL
          AND setup_complete = true;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return [dict(row) for row in cur.fetchall()]


def get_new_listings_in_bounds(conn, bounds_wkt: str, days_back: int = 10) -> pd.DataFrame:

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
    
    query = """
        SELECT 
            mls_id,
            date_collected,
            description,
            bedrooms,
            bathrooms,
            size_sqft,
            stories,
            house_cat,
            price,
            address_number,
            street_name,
            full_street_name,
            municipality,
            province_state,
            postal_code,
            pool_mentioned,
            pool_type,
            lat,
            lon
        FROM listing
        WHERE date_collected >= %s
          AND pool_mentioned = true
          AND ST_Contains(
              ST_GeomFromText(%s, 4326),
              ST_SetSRID(ST_MakePoint(lon, lat), 4326)
          )
        ORDER BY date_collected DESC;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (cutoff_date, bounds_wkt))
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    
    return pd.DataFrame(rows, columns=columns)


def get_removed_listings_in_bounds(conn, bounds_wkt: str, days_back: int = 10) -> pd.DataFrame:

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
    
    query = """
        SELECT 
            l.mls_id,
            l.date_collected as original_date_collected,
            r.removal_date,
            l.description,
            l.bedrooms,
            l.bathrooms,
            l.size_sqft,
            l.stories,
            l.house_cat,
            l.price,
            l.address_number,
            l.street_name,
            l.full_street_name,
            l.municipality,
            l.province_state,
            l.postal_code,
            l.pool_mentioned,
            l.pool_type,
            l.lat,
            l.lon
        FROM listing_removal r
        JOIN listing l ON r.mls_id = l.mls_id
        WHERE r.removal_date >= %s
          AND l.pool_mentioned = true
          AND ST_Contains(
              ST_GeomFromText(%s, 4326),
              ST_SetSRID(ST_MakePoint(l.lon, l.lat), 4326)
          )
        ORDER BY r.removal_date DESC;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (cutoff_date, bounds_wkt))
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    
    return pd.DataFrame(rows, columns=columns)


def create_xlsx_file(df: pd.DataFrame, sheet_name: str = "Listings") -> BytesIO:

    buffer = BytesIO()
    
    # Remove timezone from datetime columns for Excel compatibility
    df_copy = df.copy()
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].dt.tz_localize(None)
    
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df_copy.to_excel(writer, sheet_name=sheet_name, index=False)
    
    buffer.seek(0)
    return buffer


def send_email_with_attachments(
    to_email: str,
    subject: str,
    html_body: str,
    attachments: List[Dict[str, Any]]
) -> Dict[str, Any]:

    if not RESEND_API_KEY:
        raise ValueError("RESEND_API_KEY not found in environment")
    
    # Prepare attachments for Resend API
    api_attachments = []
    for att in attachments:
        content = att['content']
        
        # Convert BytesIO to bytes if needed
        if isinstance(content, BytesIO):
            content = content.getvalue()
        
        # Base64 encode the content
        content_b64 = base64.b64encode(content).decode('utf-8')
        
        api_attachments.append({
            'filename': att['filename'],
            'content': content_b64,
            'content_type': att.get('content_type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        })
    
    # Prepare API request
    payload = {
        'from': 'Weekly Listings Report <noreply@zenithvision.ca>',  # Update with your verified domain
        'to': [to_email],
        'subject': subject,
        'html': html_body,
        'attachments': api_attachments
    }
    
    headers = {
        'Authorization': f'Bearer {RESEND_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Send request to Resend API
    response = requests.post(
        'https://api.resend.com/emails',
        json=payload,
        headers=headers
    )
    
    response.raise_for_status()
    return response.json()


def generate_email_html(service_area: str, new_count: int, removed_count: int, license_id: int) -> str:
    # Branding colors
    primary_color = "#00004d"
    accent_color = "#e30b21"
    
    # Load logo
    logo_path = os.path.join(os.path.dirname(__file__), 'logo.png')
    logo_html = ""
    if os.path.exists(logo_path):
        try:
            with open(logo_path, "rb") as f:
                logo_b64 = base64.b64encode(f.read()).decode('utf-8')
                logo_html = f'<img src="data:image/png;base64,{logo_b64}" alt="ZenithVision Logo" style="max-height: 80px;"/>'
        except Exception as e:
            print(f"Warning: Failed to load logo: {e}")
            logo_html = "<h1>ZenithVision</h1>"
    else:
        # Fallback if logo not found
        logo_html = "<h1>ZenithVision</h1>"

    report_date = datetime.now().strftime("%B %d, %Y")
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; background-color: #f9f9f9; }}
            .container {{ max_width: 600px; margin: 0 auto; background-color: #ffffff; }}
            .header {{ background-color: #ffffff; padding: 20px; text-align: center; border-bottom: 3px solid {primary_color}; }}
            .content {{ padding: 30px; }}
            .summary {{ background-color: #f8f8f8; padding: 20px; border-left: 5px solid {accent_color}; margin: 25px 0; }}
            .summary-item {{ margin: 8px 0; font-size: 15px; }}
            .footer {{ background-color: {primary_color}; color: #ffffff; padding: 20px; text-align: center; font-size: 12px; }}
            .footer a {{ color: #ffffff; text-decoration: none; }}
            h2 {{ color: {primary_color}; margin-top: 0; }}
            .highlight {{ color: {accent_color}; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                {logo_html}
            </div>
            <div class="content">
                <p>Hello,</p>
                <p>Here is your real estate report for <span class="highlight">properties with pools</span> in your licensed area over the past 10 days.</p>
                
                <div class="summary">
                    <h2>Summary</h2>
                    <div class="summary-item"><strong>Service Area:</strong> {service_area}</div>
                    <div class="summary-item"><strong>New Pool Listings:</strong> {new_count}</div>
                    <div class="summary-item"><strong>Removed Pool Listings:</strong> {removed_count}</div>
                    <div class="summary-item"><strong>Report Date:</strong> {report_date}</div>
                </div>
                
                <p>Please find the detailed listings attached as Excel files.</p>
                <p><strong>Note:</strong> This report only includes properties with private pools (in-ground or above-ground).</p>
                <p>If you have any questions, please simply reply to this email.</p>
                
                <p style="margin-top: 30px;">Best regards,<br><strong>The ZenithVision Team</strong></p>
            </div>
            <div class="footer">
                <p>&copy; {datetime.now().year} ZenithVision. All rights reserved.</p>
                <p><a href="https://www.zenithvision.ca">www.zenithvision.ca</a></p>
                <p>License ID: #{license_id}</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html


def process_license(license_data: Dict[str, Any], listing_conn) -> Dict[str, Any]:

    license_id = license_data['id']
    admin_email = license_data['admin_email']
    bounds_wkt = license_data['bounds_wkt']
    service_area = license_data.get('service_area', 'N/A')
    
    print(f"Processing license {license_id} for {admin_email} ({service_area})...")
    
    # Get new and removed listings (with pools only)
    new_listings_df = get_new_listings_in_bounds(listing_conn, bounds_wkt, days_back=10)
    removed_listings_df = get_removed_listings_in_bounds(listing_conn, bounds_wkt, days_back=10)
    
    new_count = len(new_listings_df)
    removed_count = len(removed_listings_df)
    
    print(f"  Found {new_count} new listings with pools, {removed_count} removed listings with pools")
    
    # Skip if no listings to report
    if new_count == 0 and removed_count == 0:
        print(f"  No pool listings to report, skipping email")
        return {
            'license_id': license_id,
            'admin_email': admin_email,
            'new_count': 0,
            'removed_count': 0,
            'email_sent': False
        }
    
    # Create XLSX files
    attachments = []
    
    if new_count > 0:
        new_xlsx = create_xlsx_file(new_listings_df, sheet_name="New Pool Listings")
        attachments.append({
            'filename': f'new_pool_listings_{datetime.now().strftime("%Y%m%d")}.xlsx',
            'content': new_xlsx
        })
    
    if removed_count > 0:
        removed_xlsx = create_xlsx_file(removed_listings_df, sheet_name="Removed Pool Listings")
        attachments.append({
            'filename': f'removed_pool_listings_{datetime.now().strftime("%Y%m%d")}.xlsx',
            'content': removed_xlsx
        })
    
    # Generate email body with branding
    html_body = generate_email_html(service_area, new_count, removed_count, license_id)
    
    # Send email
    subject = f"Pool Listings Report - {new_count} New, {removed_count} Sold"
    
    try:
        result = send_email_with_attachments(
            to_email=admin_email,
            subject=subject,
            html_body=html_body,
            attachments=attachments
        )
        
        print(f"  ✓ Email sent successfully (ID: {result.get('id')})")
        
        return {
            'license_id': license_id,
            'admin_email': admin_email,
            'new_count': new_count,
            'removed_count': removed_count,
            'email_sent': True,
            'email_id': result.get('id')
        }
        
    except Exception as e:
        print(f"  ✗ Failed to send email: {e}")
        return {
            'license_id': license_id,
            'admin_email': admin_email,
            'new_count': new_count,
            'removed_count': removed_count,
            'email_sent': False,
            'error': str(e)
        }


def send_weekly_reports(master_conn, listing_conn) -> Dict[str, Any]:

    print("=" * 80)
    print("WEEKLY LISTINGS REPORT - EMAIL DISPATCH")
    print("=" * 80)
    print()
    
    # Get all licenses
    print("Fetching active licenses...")
    licenses = get_all_licenses(master_conn)
    print(f"Found {len(licenses)} active licenses")
    print()
    
    # Process each license
    results = []
    for license_data in licenses:
        result = process_license(license_data, listing_conn)
        results.append(result)
        print()
    
    # Summary statistics
    total_licenses = len(results)
    emails_sent = sum(1 for r in results if r['email_sent'])
    emails_failed = total_licenses - emails_sent
    total_new = sum(r['new_count'] for r in results)
    total_removed = sum(r['removed_count'] for r in results)
    
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total licenses processed: {total_licenses}")
    print(f"Emails sent successfully: {emails_sent}")
    print(f"Emails failed: {emails_failed}")
    print(f"Total new listings reported: {total_new}")
    print(f"Total removed listings reported: {total_removed}")
    print()
    
    return {
        'total_licenses': total_licenses,
        'emails_sent': emails_sent,
        'emails_failed': emails_failed,
        'total_new_listings': total_new,
        'total_removed_listings': total_removed,
        'results': results
    }
