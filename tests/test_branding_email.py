import os
import sys
import base64
from datetime import datetime
from io import BytesIO
import pandas as pd
from dotenv import load_dotenv

# Add the include/inform directory to the path so we can import send_reports
current_dir = os.path.dirname(os.path.abspath(__file__))
inform_dir = os.path.join(current_dir, '../include/inform')
sys.path.append(inform_dir)

import send_reports

# Load environment variables (for RESEND_API_KEY)
load_dotenv(os.path.join(current_dir, '../../.env'))

def create_dummy_excel(filename):
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    return {'filename': filename, 'content': buffer}

def run_test():
    print("Starting branding email test...")
    
    recipient = "james.hutchins2@outlook.com"
    subject = "Refined Branding Test - ZenithVision"
    
    # Mock Data
    service_area = "Test Region - Moncton"
    new_count = 12
    removed_count = 5
    license_id = 101
    
    # Check if generate_email_html exists (after refactor)
    if hasattr(send_reports, 'generate_email_html'):
        print("Generating HTML using new refactored function...")
        html_body = send_reports.generate_email_html(
            service_area=service_area,
            new_count=new_count,
            removed_count=removed_count,
            license_id=license_id
        )
    else:
        print("generate_email_html not found. Please refactor send_reports.py first.")
        return

    # Create dummy attachments
    attachments = [
        create_dummy_excel("new_listings_test.xlsx"),
        create_dummy_excel("removed_listings_test.xlsx")
    ]
    
    print(f"Sending email to {recipient}...")
    try:
        result = send_reports.send_email_with_attachments(
            to_email=recipient,
            subject=subject,
            html_body=html_body,
            attachments=attachments
        )
        print(f"Email sent successfully! ID: {result.get('id')}")
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    run_test()
