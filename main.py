import os
import gspread
from google.oauth2.service_account import Credentials
import functions_framework
import json
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Google API Scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Global variable for the gspread client to allow connection reuse
sheet_client = None

def get_sheet_client():
    """Initializes and returns a gspread client, reusing the connection if available."""
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

@functions_framework.http
def update_google_sheet(request):
    """
    A single webhook to handle updates from both inbound and outbound Dialogflow agents.
    - For 'outbound' calls, it updates a specific row.
    - For 'inbound' calls, it appends a new row.
    """
    # Handle CORS preflight requests
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {'Access-Control-Allow-Origin': '*'}
    if request.method != 'POST':
        return ({'error': 'Method Not Allowed, use POST'}, 405, headers)

    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            raise ValueError("Invalid or empty JSON body in request")
        
        params = request_json.get('toolInfo', {}).get('parameters', request_json)
        call_type = params.get('callType')

        if not call_type:
            raise ValueError("'callType' parameter ('inbound' or 'outbound') is required.")

        client = get_sheet_client()

        # --- LOGIC FOR OUTBOUND CALLS ---
        if call_type == 'outbound':
            spreadsheet_id = os.environ.get("SPREADSHEET_ID") # Your original outbound sheet ID
            if not spreadsheet_id:
                raise ValueError("SPREADSHEET_ID environment variable not set.")
            worksheet = client.open_by_key(spreadsheet_id).sheet1
            
            row_index = params.get('sheetRowIndex')
            if row_index is None:
                raise ValueError("sheetRowIndex is a required parameter for outbound calls.")

            column_mapping = {
                'callStatus': 5, 'callSummary': 6, 'appointmentDate': 7,
                'appointmentTime': 8, 'emailID': 9
            }
            for param_name, col_num in column_mapping.items():
                if param_name in params and params[param_name] is not None:
                    worksheet.update_cell(int(row_index), col_num, str(params[param_name]))
            logging.info(f"Updated outbound sheet for row {row_index}")

        # --- LOGIC FOR INBOUND CALLS ---
        elif call_type == 'inbound':
            spreadsheet_id = os.environ.get("INBOUND_SPREADSHEET_ID") # Your new inbound sheet ID
            if not spreadsheet_id:
                raise ValueError("INBOUND_SPREADSHEET_ID environment variable not set.")
            worksheet = client.open_by_key(spreadsheet_id).sheet1

            inbound_data = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                params.get('customerName', 'N/A'),
                params.get('callerPhone', 'N/A'),
                params.get('businessType', 'N/A'),
                params.get('details', 'N/A'),
                params.get('callSummary', 'N/A')
            ]
            worksheet.append_row(inbound_data)
            logging.info(f"Appended new row to inbound sheet for {params.get('customerName')}")

        else:
            raise ValueError(f"Invalid callType: '{call_type}'. Must be 'inbound' or 'outbound'.")

        # --- SUCCESS RESPONSE ---
        response_payload = {
            "tool_response": [{"tool_output": {"status": "SUCCESS", "message": f"{call_type.capitalize()} data processed."}}]
        }
        return (response_payload, 200, headers)

    except Exception as e:
        logging.error(f"❌ Exception: {e}")
        error_payload = {
            "tool_response": [{"tool_output": {"status": "ERROR", "message": str(e)}}]
        }
        return (error_payload, 200, headers)
