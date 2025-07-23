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

# (Your get_sheet_client and get_sheet functions remain the same)
# ...

@functions_framework.http
def update_google_sheet(request):
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

    # --- Expect a POST request, not GET ---
    if request.method != 'POST':
        return ({'error': 'Method Not Allowed'}, 405, headers)

    try:
        # --- Read parameters from the JSON body ---
        request_json = request.get_json(silent=True)
        if not request_json:
            return ({'error': 'Invalid JSON body'}, 400, headers)
        
        # In Dialogflow CX, parameters are nested under 'toolInfo'.'parameters'
        # In Vertex AI Agent Builder, they might be at the top level.
        # This code checks for the more common CX structure first.
        params = request_json.get('toolInfo', {}).get('parameters', request_json)
        
        row_index = params.get('sheetRowIndex')
        if not row_index:
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
            if param_name in params:
                worksheet.update_cell(int(row_index), col_num, params[param_name])
                logging.info(f"Updated row {row_index}, column {col_num} with value: {params[param_name]}")
        
        # --- ✅ The required JSON response for Dialogflow ---
        response_payload = {
            "tool_response": [{
                "tag": "updateSheet", # This tag should match the operationId
                "tool_output": {
                    "status": "SUCCESS",
                    "message": f"Sheet successfully updated for row {row_index}"
                }
            }]
        }
        return (response_payload, 200, headers)

    except Exception as e:
        logging.error(f"❌ Exception: {e}")
        error_payload = {
            "tool_response": [{
                "tag": "updateSheet",
                "tool_output": {
                    "status": "ERROR",
                    "message": str(e)
                }
            }]
        }
        # Return 200 OK but with an error status inside the JSON for Dialogflow
        return (error_payload, 200, headers)
