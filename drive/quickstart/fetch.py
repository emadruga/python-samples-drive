# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START drive_quickstart]
from __future__ import print_function

import os.path
import io
import re
import logging

from tqdm import tqdm
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# If modifying these scopes, delete the file token.json.
SCOPES1 = ['https://www.googleapis.com/auth/drive.metadata.readonly',
           'https://www.googleapis.com/auth/drive']
SCOPES2 = ['https://www.googleapis.com/auth/spreadsheets.readonly']

NOME_PASTA_DESAFIO = '2023-1 :: Desafio 1'
DEST_DIR = 'Challenges/E01'

# The range of target spreadsheet.
# SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
SAMPLE_RANGE_NAME = 'A2:W36'


def download_file(drive_service, file_id, name, filename):
    # download a file as defined by the drive service file id
    percentage_file_received = 0
    try:
        request = drive_service.files().get_media(fileId=file_id)

        if not os.path.isdir(f'{DEST_DIR}/{name}'):
            logging.debug(f'Creating folder: {DEST_DIR}/{name}...')
            os.makedirs(f'{DEST_DIR}/{name}')

        fh = io.FileIO(f'{DEST_DIR}/{name}/{filename}', 'wb')  # this can be used to write to disk

        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            percentage_file_received = int(status.progress() * 100)
            logging.debug("Download %d%%." % percentage_file_received)

    except HttpError as err:
        raise HttpError(f'File download failure: {filename} ({err})')

    return percentage_file_received


def process_row(drive_service, row):
    # Print columns, which correspond to indices 0 and 4.
    perc_file_read = 0
    timestamp = row[0]
    name = row[2]
    file_url = row[-1]
    if "https://" not in file_url:
        raise ValueError(f'Invalid URL for submission {name}: {file_url}')
    logging.debug('%s, %s' % (name, file_url))
    file_id = re.search(r".*id=(.*)$", file_url).group(1)
    try:
        # https://developers.google.com/drive/api/v3/reference/files
        # https://developers.google.com/drive/api/v2/reference/files/get#python
        # https://developers.google.com/drive/api/v3/reference/replies/get
        file = drive_service.files().get(fileId=file_id,
                                         fields='id,name,createdTime,size').execute()
        filename = file.get("name")
        file_size = int(file.get("size"))
        logging.debug(F'Found file: {file.get("name")}, {file_size} bytes,{file.get("id")}, {timestamp}')

        if file_size == 0:
            raise EOFError(f'File {filename}: empty...')

        perc_file_read = download_file(drive_service, file_id, name, filename)
    except HttpError as err:
        raise HttpError(f'File error: {err}')
    except EOFError as err:
        raise EOFError(err)

    return perc_file_read


def compute_range(rows, cols, forced_rows):
    # our sheet data always starts at cell 'A2'
    # need to come up with the last column and row
    sheet_range = ''
    col_names = []
    extras = []
    for pos in range(26):
        curr = chr(ord('A') + pos)
        col_names.append(curr)
        extras.append(f'A{curr}')
    # for the moment, all_columns contains columns A-Z + AA-AZ (size 52)
    all_columns = col_names + extras

    idx = min(cols, len(all_columns)) - 1
    # It appears that Google Sheets adds 6 cols to the right of the actual last column
    end_column_letter = all_columns[idx-6]

    # It appears that Google Sheets adds 100 to the last row that is non-empty
    end_row_num = max(rows - 100, forced_rows)
    sheet_range = f'A2:{end_column_letter}{end_row_num}'
    return sheet_range


def process_sheet_rows(drive_service, sheet_service, spreadsheet_id, forced_rows):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    num_submissions_ok = 0
    try:
        sss = sheet_service.spreadsheets()
        # https://developers.google.com/sheets/api/samples/sheet
        props = sss.get(spreadsheetId=spreadsheet_id,
                        fields="sheets.properties").execute()
        sheet = props.get('sheets', {})[0]
        rows = sheet['properties']['gridProperties']['rowCount']
        cols = sheet['properties']['gridProperties']['columnCount']
        logging.debug(f'Given Shape {rows}:{cols}')
        srange = compute_range(rows, cols, forced_rows)
        logging.info(f'Presumed Shape {srange}')
        result = sss.values().get(spreadsheetId=spreadsheet_id, range=srange).execute()
        rows = result.get('values', [])

        if not rows:
            logging.debug('No data found.')
            return

        row_exceptions = []
        logging.debug(f'#Possible submissions: {len(rows)}')
        for row in tqdm(range(len(rows)), desc="Downloading..."):
            percentage_file_received = 0
            submission = rows[row]
            author = submission[2]
            max_attemps = 5
            while percentage_file_received < 100 and max_attemps > 0:
                try:
                    percentage_file_received = process_row(drive_service, submission)
                    if percentage_file_received >= 100:
                        num_submissions_ok += 1
                    else:
                        max_attemps -= 1
                    if max_attemps == 0:
                        logging.warning(f'File {author}: too long. Giving up...')
                except EOFError as err:
                    row_exceptions.append((author, err))
                    break

    except HttpError as err:
        logging.error(err)

    logging.info('Files to watch:')
    for exc in row_exceptions:
        author = exc[0]
        reason_exception = exc[1]
        logging.info(f'{author}: {reason_exception}')
    return num_submissions_ok


def main(forced_rows):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES1)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES1)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        drive_service = build('drive', 'v3', credentials=creds)
        sheet_service = build('sheets', 'v4', credentials=creds)
        num_submissions_processed = 0

        logging.info(f'Looking into Google Drive folder "{NOME_PASTA_DESAFIO}"...')
        # Call the Drive v3 API
        folders_from_name = drive_service.files().list(
            q=f"mimeType = 'application/vnd.google-apps.folder' and name = '{NOME_PASTA_DESAFIO}'",
            pageSize=20, fields="nextPageToken, files(id, name)").execute()

        # this gives us a list of all folders with that name
        folders = folders_from_name.get('files', [])
        # however, we know there is only 1 folder with that name, so we just get the id of the 1st item in the list
        folder_id = folders[0].get('id')

        # Now, using the folder ID gotten above, we get all the files from
        # that particular folder
        files_given_folder = drive_service.files().list(q="'" + folder_id + "' in parents", pageSize=10,
                                                        fields="nextPageToken, files(id, name)").execute()
        files = files_given_folder.get('files', [])

        if not files:
            logging.info('No files found.')
            return

        for file in files:
            # Now we process the Google Sheet file inside the chosen folder
            if "(respostas)" in file['name']:
                logging.info(f'Sheet file to process: {file["name"]}')
                logging.info(f'Saving submissions to: {DEST_DIR}')
                num_submissions_processed = process_sheet_rows(drive_service, sheet_service, file['id'], forced_rows)
                logging.info(f'#Downloads Ok: {num_submissions_processed}')

        if num_submissions_processed == 0:
            logging.info(f'No submissions found: Google Sheet file in the right folder?')

    except HttpError as error:
        # TODO(developer) - Handle errors from sheet API.
        logging.error(f'An error occurred: {error}')


if __name__ == '__main__':

    # if rows were manually added, for authors with very good reasons...
    forced_rows = 60
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S',
                        level=logging.INFO)
    main(forced_rows)
# [END drive_quickstart]
