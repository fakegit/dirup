from google.oauth2.service_account import Credentials as SACredentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaFileUpload
import googleapiclient.discovery, argparse, pathlib, progress.bar, os, hashlib, pickle

parser = argparse.ArgumentParser(description="tool to mass upload google drive files")
parser.add_argument("path", help="path to files")
parser.add_argument("dest", help="fileid destination to upload to")
parser.add_argument("--key", "-k", help="path to key file", required=False, default="key.json")
parser.add_argument("-h", help="use a human account", required=False, action="store_true")
args = parser.parse_args()

if not args.h:
    creds = SACredentials.from_service_account_file(args.key, scopes=[
        "https://www.googleapis.com/auth/drive"
    ])
else:
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                args.key, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

drive = googleapiclient.discovery.build("drive", "v3", credentials=creds)

def ls(parent, searchTerms=""):
    files = []
    resp = drive.files().list(q=f"'{parent}' in parents" + searchTerms, pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files += resp["files"]
    while "nextPageToken" in resp:
        resp = drive.files().list(q=f"'{parent}' in parents" + searchTerms, pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True, pageToken=resp["nextPageToken"]).execute()
        files += resp["files"]
    return files

def lsd(parent):
    
    return ls(parent, searchTerms=" and mimeType contains 'application/vnd.google-apps.folder'")

def lsf(parent):
    
    return ls(parent, searchTerms=" and not mimeType contains 'application/vnd.google-apps.folder'")

def drive_path(path, parent):
    
    files = lsd(parent)
    for i in files:
        if i["name"] == path[0]:
            if len(path) == 1:
                return i["id"]
            else:
                return drive_path(path[1:], i["id"])
    resp = drive.files().create(body={
        "mimeType": "application/vnd.google-apps.folder",
        "name": path[0],
        "parents": [parent]
    }, supportsAllDrives=True).execute()
    if len(path) == 1:
        return resp["id"]
    else:
        return drive_path(path[1:], resp["id"])

def md5sum(filename):
    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
            md5.update(chunk)
    return md5.hexdigest()

def upload_resumable(filename, parent):
    
    media = MediaFileUpload(filename, resumable=True)
    request = drive.files().create(media_body=media, supportsAllDrives=True, body={
        "name": filename.split("/")[-1],    
        "parents": [parent]
    })
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print("Uploaded {:02.0f}%".format(status.progress()*100.0))
    return response

def upload_multipart(filename, parent):
    
    media = MediaFileUpload(filename)
    request = drive.files().create(media_body=media, supportsAllDrives=True, body={
        "name": filename.split("/")[-1],    
        "parents": [parent]
    }).execute()
    return request

files = [i for i in pathlib.Path(args.path).glob("**/*") if not i.is_dir()]
dirs_processed = []
pbar = progress.bar.Bar("processing files", max=len(files))
for i in files:
    file_path = i.as_posix()
    file_dir = "/".join(file_path.split("/")[:-1])
    
    flag = False
    for o in dirs_processed:
        if o[0] == file_dir:
            o[2].append(file_path)
            pbar.next()
            flag = True
            break
    if flag:
        continue
    dirs_processed.append([file_dir, drive_path(file_dir.split("/"), args.dest), [file_path]])
    pbar.next()
pbar.finish()

pbar = progress.bar.Bar("checking for dupes", max=len(dirs_processed))
deduped = []
for i in dirs_processed:
    dir_contents = lsf(i[1])
    tmp = []
    for o in i[2]:
        file_name = o.split("/")[-1]
        isdupe = False
        for p in dir_contents:
            if file_name == p["name"]:
                local_md5 = md5sum(o)
                remote_md5 = drive.files().get(fileId=p["id"], fields="md5Checksum", supportsAllDrives=True).execute()["md5Checksum"]
                if local_md5 != remote_md5:
                    drive.files().delete(fileId=p["id"], supportsAllDrives=True).execute()
                else:
                    isdupe = True
                break
        if not isdupe:
            tmp.append(o)
    deduped.append([i[0], i[1], tmp])
    pbar.next()
pbar.finish()

for i in deduped:
    for o in i[2]:
        print("uploading " + o)
        fsize = os.stat(o).st_size
        local_md5 = md5sum(o)
        while True: # keep retrying until it uploads correctly
            if fsize == 0: # if file empty, just create it
                resp = drive.files().create(body={
                    "name": o.split[-1],
                    "parents": [i[1]]
                }, supportsAllDrives=True).execute()
                break
            elif fsize <= 5120: # if file 5MB or lower use multipart upoad
                resp = upload_multipart(o, i[1])
            else: # if files size above 5MB user resumable
                resp = upload_resumable(o, i[1])
            remote_md5 = drive.files().get(fileId=resp["id"], fields="md5Checksum", supportsAllDrives=True).execute()["md5Checksum"]
            if remote_md5 != local_md5: # if upload has wrong md5
                drive.files().delete(fileId=resp["id"], supportsAllDrives=True).execute()
            else:
                break