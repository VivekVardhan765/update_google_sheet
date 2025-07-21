import os
import gspread
from google.oauth2.service_account import Credentials
import functions_framework
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Google API Scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

sheet_client = None
sheet = None

def get_sheet_client():
    global sheet_client
    if sheet_client is None:
        try:
            creds_json = os.environ.get("GCP_SERVICE_ACCOUNT_KEY_JSON")
            if not creds_json:
                raise ValueError("Missing GCP_SERVICE_ACCOUNT_KEY_JSON environment variable.")

            creds_info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
            sheet_client = gspread.authorize(creds)
            logging.info("✅ Google Sheets client initialized.")
        except Exception as e:
            logging.error(f"❌ Failed to initialize client: {e}")
            raise
    return sheet_client

def get_sheet():
    global sheet
    if sheet is None:
        try:
            spreadsheet_id = os.environ.get("SPREADSHEET_ID")
            if not spreadsheet_id:
                raise ValueError("Missing SPREADSHEET_ID environment variable.")
            client = get_sheet_client()
            sheet = client.open_by_key(spreadsheet_id).sheet1
            logging.info("✅ Opened target sheet.")
        except Exception as e:
            logging.error(f"❌ Failed to open sheet: {e}")
            raise
    return sheet

@functions_framework.http
def update_google_sheet(request):
    if request.method != 'GET':
        return 'Method Not Allowed', 405

    args = request.args
    required_fields = ['sheetRowIndex']
    optional_fields = {
        'callStatus': 5,
        'callSummary': 6,
        'appointmentDate': 7,
        'appointmentTime': 8,
        'emailID': 9
    }

    # Validate required field
    if not args.get('sheetRowIndex'):
        return 'Bad Request: sheetRowIndex is required', 400

    try:
        row_index = int(args.get('sheetRowIndex'))
    except ValueError:
        return 'Bad Request: sheetRowIndex must be an integer', 400

    try:
        worksheet = get_sheet()
        updates = {}
        for param, col in optional_fields.items():
            if args.get(param) is not None:
                updates[col] = args.get(param)

        if not updates:
            return 'No valid updates provided', 200

        for col, val in updates.items():
            worksheet.update_cell(row_index, col, val)
            logging.info(f"Updated row {row_index}, column {col}: {val}")

        return 'Google Sheet updated successfully', 200

    except Exception as e:
        logging.error(f"❌ Exception: {e}")
        return f'Internal Server Error: {e}', 500
