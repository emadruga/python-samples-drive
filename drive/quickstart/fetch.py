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

NOME_PASTA_DESAFIO = 'Desafio 8'
DEST_DIR = './Files'

# The range of target spreadsheet.
# SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
SAMPLE_RANGE_NAME = 'A2:W36'


def download_file(drive_service, file_id, name, filename):
    # download a file as defined by the drive service file id
    try:
        request = drive_service.files().get_media(fileId=file_id)

        if not os.path.isdir(f'{DEST_DIR}/{name}'):
            print(f'Creating folder: {DEST_DIR}/{name}...')
            os.makedirs(f'{DEST_DIR}/{name}')

        fh = io.FileIO(f'{DEST_DIR}/{name}/{filename}', 'wb')  # this can be used to write to disk

        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))

    except HttpError as err:
        print(f'File download failure: {err}')
        raise


def process_row(drive_service, row):
    # Print columns, which correspond to indices 0 and 4.
    timestamp = row[0]
    name = row[2]
    file_url = row[-1]
    if "https://" not in file_url:
        raise ValueError(f'Invalid URL for submission {name}: {file_url}')
    print('%s, %s' % (name, file_url))
    file_id = re.search(r".*id=(.*)$", file_url).group(1)
    try:
        # https://developers.google.com/drive/api/v3/reference/files
        # https://developers.google.com/drive/api/v2/reference/files/get#python
        # https://developers.google.com/drive/api/v3/reference/replies/get
        file = drive_service.files().get(fileId=file_id,
                                         fields='id,name,createdTime,size').execute()
    except HttpError as err:
        print(f'File metadata failure: {err}')
        raise

    print(F'Found file: {file.get("name")}, {file.get("size")} bytes,{file.get("id")}, {timestamp}')
    filename = file.get("name")
    download_file(drive_service, file_id, name, filename)


def compute_range(rows, cols):
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
    end_row_num = rows - 100
    sheet_range = f'A2:{end_column_letter}{end_row_num}'
    return sheet_range


def process_sheet_rows(drive_service, sheet_service, spreadsheet_id):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    try:
        sss = sheet_service.spreadsheets()
        # https://developers.google.com/sheets/api/samples/sheet
        props = sss.get(spreadsheetId=spreadsheet_id,
                        fields="sheets.properties").execute()
        sheet = props.get('sheets', {})[0]
        rows = sheet['properties']['gridProperties']['rowCount']
        cols = sheet['properties']['gridProperties']['columnCount']
        print(f'Sheet Shape {rows}:{cols}')
        srange = compute_range(rows, cols)
        result = sss.values().get(spreadsheetId=spreadsheet_id, range=srange).execute()
        rows = result.get('values', [])

        if not rows:
            print('No data found.')
            return

        print(f'Aluno, File URL ({len(rows)}):')
        for submission in rows:
            process_row(drive_service, submission)

    except HttpError as err:
        print(err)


def main():
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
            print('No files found.')
            return

        for file in files:
            # Now we process the Google Sheet file inside the chosen folder
            if "(respostas)" in file['name']:
                print(u'{0} ({1})'.format(file['name'], file['id']))
                process_sheet_rows(drive_service, sheet_service, file['id'])

    except HttpError as error:
        # TODO(developer) - Handle errors from sheet API.
        print(f'An error occurred: {error}')


if __name__ == '__main__':
    main()
# [END drive_quickstart]
