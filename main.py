import os
import gspread
from google.oauth2.service_account import Credentials
import functions_framework
import json
import logging

# Configure logging for Cloud Run
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==================== CONFIGURATION (FOR CLOUD RUN ENVIRONMENT VARIABLES) ====================
# SPREADSHEET_ID and GCP_SERVICE_ACCOUNT_KEY_JSON will be loaded from Cloud Run environment variables
# Ensure these are set correctly in your Cloud Run service's "Variables & Secrets" section.

# Defines the permissions the script needs for Google Sheets/Drive APIs
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Initialize gspread client and sheet globally to reuse connections across invocations
sheet_client = None
sheet = None

def get_sheet_client():
    """Initializes and returns the gspread client using service account JSON from environment."""
    global sheet_client
    if sheet_client is None:
        try:
            # Load service account key JSON from environment variable
            gcp_service_account_key_json = os.environ.get("GCP_SERVICE_ACCOUNT_KEY_JSON")
            if not gcp_service_account_key_json:
                raise ValueError("GCP_SERVICE_ACCOUNT_KEY_JSON environment variable not set.")

            creds_info = json.loads(gcp_service_account_key_json)
            creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
            sheet_client = gspread.authorize(creds)
            logging.info("✅ Successfully connected to Google Sheet client.")
        except Exception as e:
            logging.error(f"❌ Error initializing gspread client: {e}")
            raise
    return sheet_client

def get_sheet():
    """Opens the specific sheet and returns the first worksheet."""
    global sheet
    if sheet is None:
        try:
            spreadsheet_id = os.environ.get("SPREADSHEET_ID")
            if not spreadsheet_id:
                raise ValueError("SPREADSHEET_ID environment variable not set.")
            client = get_sheet_client()
            sheet = client.open_by_key(spreadsheet_id).sheet1
            logging.info(f"✅ Successfully opened Google Sheet with ID: {spreadsheet_id}")
        except Exception as e:
            logging.error(f"❌ Error opening Google Sheet: {e}")
            raise
    return sheet

@functions_framework.http
def update_google_sheet(request):
    """
    HTTP Cloud Run function to update Google Sheet based on Twilio Studio webhook.
    This function expects a POST request with a JSON payload containing
    the sheetRowIndex and the playbook output parameters.

    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    # Ensure the request method is POST
    if request.method != 'POST':
        logging.warning(f"Method Not Allowed: Received {request.method} request.")
        return 'Method Not Allowed', 405

    # Parse the JSON payload from the request body
    request_json = request.get_json(silent=True)
    if not request_json:
        logging.error("Bad Request: JSON payload missing or invalid.")
        return 'Bad Request: JSON payload missing or invalid', 400

    logging.info(f"Received payload: {json.dumps(request_json, indent=2)}")

    # Extract parameters from the incoming webhook payload.
    # These keys must match what you send from Twilio Studio's HTTP Request widget.
    sheet_row_index = request_json.get('sheetRowIndex')
    appoint_date_time = request_json.get('appointmentDate') # Playbook output 'appointmentDate' maps to sheet's 'appointDateTime'
    appointment_time = request_json.get('appointmentTime')
    call_status = request_json.get('callStatus')
    call_summary = request_json.get('callSummary')
    email_id = request_json.get('email') # Assuming 'email' will be passed from Playbook if collected

    # Validate sheet_row_index
    if not sheet_row_index:
        logging.error("Bad Request: sheetRowIndex is required in the payload.")
        return 'Bad Request: sheetRowIndex is required', 400

    try:
        sheet_row_index = int(sheet_row_index)
    except ValueError:
        logging.error(f"Bad Request: Invalid sheetRowIndex '{sheet_row_index}'. Must be an integer.")
        return 'Bad Request: sheetRowIndex must be an integer', 400

    try:
        worksheet = get_sheet()

        # Define column mappings for your Google Sheet (1-based indexing)
        # Based on your new sheet structure (from screenshot):
        # A: Leadphone (1)
        # B: customerName (2)
        # C: businessType (3)
        # D: details (4)
        # E: appointDateTime (5)
        # F: appointmentTime (6)
        # G: callStatus (7)
        # H: callSummary (8)
        # I: emailID (9)
        COL_APPOINT_DATE_TIME = 5   # Column E
        COL_APPOINTMENT_TIME = 6    # Column F
        COL_CALL_STATUS = 7         # Column G
        COL_CALL_SUMMARY = 8        # Column H
        COL_EMAIL_ID = 9            # Column I

        updates = {}
        # Populate updates dictionary only if data is present in the payload
        if appoint_date_time is not None:
            updates[COL_APPOINT_DATE_TIME] = appoint_date_time
        if appointment_time is not None:
            updates[COL_APPOINTMENT_TIME] = appointment_time
        if call_status is not None:
            updates[COL_CALL_STATUS] = call_status
        if call_summary is not None:
            updates[COL_CALL_SUMMARY] = call_summary
        if email_id is not None: # Only update emailID if the email parameter was provided
            updates[COL_EMAIL_ID] = email_id

        if not updates:
            logging.info(f"No valid update parameters provided in the payload for row {sheet_row_index}.")
            return 'No valid updates provided', 200 # Still return 200 if nothing to update

        # Perform updates for each column
        for col, value in updates.items():
            # gspread's update_cell is efficient for single cell updates
            worksheet.update_cell(sheet_row_index, col, value)
            logging.info(f"Updated row {sheet_row_index}, column {col} with value: '{value}'")

        logging.info(f"Successfully processed and updated Google Sheet for row {sheet_row_index}.")
        return 'Google Sheet updated successfully', 200

    except Exception as e:
        logging.error(f"❌ Error updating Google Sheet for row {sheet_row_index}: {e}")
        return f'Internal Server Error: {e}', 500

