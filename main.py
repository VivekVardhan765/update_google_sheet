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

# --- These global variables help with connection reuse ---
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
    """
    Receives a POST request from Dialogflow, updates a Google Sheet,
    and returns a structured JSON response.
    """
    # --- Handle CORS preflight requests ---
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    # --- Set standard CORS headers for the actual response ---
    headers = {'Access-Control-Allow-Origin': '*'}

    # --- Expect a POST request from Dialogflow ---
    if request.method != 'POST':
        return ({'error': 'Method Not Allowed, use POST'}, 405, headers)

    try:
        # --- Read parameters from the JSON body ---
        request_json = request.get_json(silent=True)
        if not request_json:
            raise ValueError("Invalid or empty JSON body in request")
        
        # This handles the nested parameter structure from Dialogflow CX tools
        params = request_json.get('toolInfo', {}).get('parameters', request_json)
        
        row_index = params.get('sheetRowIndex')
        if row_index is None:
            raise ValueError("sheetRowIndex is a required parameter")

        worksheet = get_sheet()
        
        column_mapping = {
            'callStatus': 5,
            'callSummary': 6,
            'appointmentDate': 7,
            'appointmentTime': 8,
            'emailID': 9
        }

        # Update cells based on provided parameters
        for param_name, col_num in column_mapping.items():
            if param_name in params and params[param_name] is not None:
                worksheet.update_cell(int(row_index), col_num, str(params[param_name]))
                logging.info(f"Updated row {row_index}, column {col_num} with value: {params[param_name]}")
        
        # --- ✅ The successful JSON response for Dialogflow ---
        response_payload = {
            "tool_response": [{
                "tag": "updateSheet",
                "tool_output": {
                    "status": "SUCCESS",
                    "message": f"Sheet successfully updated for row {row_index}"
                }
            }]
        }
        return (response_payload, 200, headers)

    except Exception as e:
        logging.error(f"❌ Exception: {e}")
        # --- 🔴 The error JSON response for Dialogflow ---
        error_payload = {
            "tool_response": [{
                "tag": "updateSheet",
                "tool_output": {
                    "status": "ERROR",
                    "message": str(e)
                }
            }]
        }
        # Return 200 OK so Dialogflow can process the error message inside
        return (error_payload, 200, headers)
