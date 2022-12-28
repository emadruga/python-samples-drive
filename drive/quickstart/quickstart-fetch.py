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
import sys
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


def get_sheet(drive_service, service, spreadsheet_id):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
            return

        print('Name, Major:')
        for row in values:
            # Print columns, which correspond to indices 0 and 4.
            timestamp = row[0]
            name = row[2]
            file_url = row[-1]
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
                sys.exit(1)
            print(F'Found file: {file.get("name")}, {file.get("size")} bytes,{file.get("id")}, {timestamp}')
            filename = file.get("name")

            request = drive_service.files().get_media(fileId=file_id)
            if not os.path.isdir(f'{DEST_DIR}/{name}'):
                print(f'Creating folder: {DEST_DIR}/{name}...')
                os.makedirs(f'{DEST_DIR}/{name}')
            # fh = io.BytesIO() # this can be used to keep in memory
            fh = io.FileIO(f'{DEST_DIR}/{name}/{filename}', 'wb')  # this can be used to write to disk
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print("Download %d%%." % int(status.progress() * 100))


    except HttpError as err:
        print(err)


def main():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    creds2 = None
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

    if os.path.exists('token2.json'):
        creds2 = Credentials.from_authorized_user_file('token2.json', SCOPES2)
    # If there are no (valid) credentials available, let the user log in.
    if not creds2 or not creds2.valid:
        if creds2 and creds2.expired and creds2.refresh_token:
            creds2.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES2)
            creds2 = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token2.json', 'w') as token2:
            token2.write(creds2.to_json())

    try:
        service = build('drive', 'v3', credentials=creds)
        sheet_service = build('sheets', 'v4', credentials=creds2)

        # Call the Drive v3 API
        folderId = service.files().list(
            q = f"mimeType = 'application/vnd.google-apps.folder' and name = '{NOME_PASTA_DESAFIO}'",
            pageSize=20, fields="nextPageToken, files(id, name)").execute()

        # this gives us a list of all folders with that name
        folderIdResult = folderId.get('files', [])
        # however, we know there is only 1 folder with that name, so we just get the id of the 1st item in the list
        id = folderIdResult[0].get('id')

        # Now, using the folder ID gotten above, we get all the files from
        # that particular folder
        results = service.files().list(q="'" + id + "' in parents", pageSize=10,
                                     fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            print('No files found.')
            return
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))

            # Now we process the Google Sheet file inside the chosen folder
            if "(respostas)" in item['name']:
                get_sheet(service, sheet_service, item['id'])

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')


if __name__ == '__main__':
    main()
# [END drive_quickstart]
