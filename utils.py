from datetime import datetime, timedelta
from googleapiclient.http import MediaIoBaseDownload
from tqdm import tqdm
from googleapiclient.http import MediaFileUpload
import os
import time
import threading

def getFullPathToId(service, fileId, targetId):
    currentId = fileId
    parthParts = []
    
    while currentId != targetId:
      file_info = service.files().get(
        fileId = currentId,
        fields = 'name,parents,trashed,explicitlyTrashed',
        supportsAllDrives = True,
      ).execute()
      
      if file_info['trashed'] or file_info['explicitlyTrashed']:
        break

      if 'parents' in file_info:
        currentId = file_info['parents'][0]
        parthParts.insert(0, file_info['name'])
      else:
        break
    
    if currentId == targetId:
      return '/'.join(parthParts)
    else:
      return None

def downloadFileAndSaveWithProgress(service, fileId, savePath, id):
  request = service.files().get_media(
    fileId = fileId,
    supportsAllDrives = True,
  )
  
  fileName = service.files().get(
    fileId = fileId,
    supportsAllDrives = True
  ).execute()['name']
  
  filePath = f"{savePath}/{fileName}"

  response = service.files().get(fileId=fileId, fields='size,name', supportsAllDrives=True).execute()
  file_size = int(response.get('size', 0))

  with open(filePath, 'wb') as f:
    downloader = MediaIoBaseDownload(f, request, chunksize=1024*1024)
    done = False
    with tqdm(
      desc = fileName,
      total = file_size,
      unit = 'B',
      unit_scale = True,
      unit_divisor = 1024
    ) as bar:
      while not done:
        status, done = downloader.next_chunk()
        percentage = status.total_size - bar.n
        bar.update(percentage)

  response = service.files().get(
    fileId = fileId,
    fields = 'id,name,mimeType,modifiedTime,parents',
    supportsAllDrives = True
  ).execute()

  response['localPath'] = filePath
  return response

def getLastThreeMinutesTimestamp():
  currenTime = datetime.now()

  threeMinutesAgo = currenTime - timedelta(minutes=3)

  formattedTimestamp = threeMinutesAgo.strftime('%Y-%m-%dT%H:%M:%S')

  return formattedTimestamp

def listActivities(service, driveId):
  lastThreeMinutes = getLastThreeMinutesTimestamp()
  
  filter = f'detail.action_detail_case:(CREATE MOVE RENAME) AND time >= "{lastThreeMinutes}.274Z"'
  
  body = {
    "ancestorName": f"items/{driveId}",
    "pageSize": 1000,
    "filter": filter
  }
  
  response = service.activity().query(
    body = body
  ).execute()
  
  activities = response.get('activities', [])
  
  return activities

def findFolderInFolder(service, parentFolderId, folderName):
  query = f"'{parentFolderId}' in parents and mimeType='application/vnd.google-apps.folder' and name='{folderName}' and trashed=false"
    
  results = service.files().list(
    q = query,
    fields = 'files(id,name, parents)',
    supportsAllDrives = True,
    supportsTeamDrives = True,
    includeTeamDriveItems = True,
  ).execute()
    
  if 'files' in results:
    for file in results['files']:
      if file['name'] == folderName:
        return file['id']
    
  return None

def listFiles(service, query, driveId):
  req = service.files().list(
    q=query,
    fields='nextPageToken, files(id,name,parents,createdTime)',
    supportsAllDrives=True,
    supportsTeamDrives=True,
    includeTeamDriveItems=True,
    pageSize=1000,
    driveId=driveId,
    corpora='drive'
  )

  res = req.execute()

  return res

def haveItem(service, query):
    page_token = None
    result = []
    
    while True:
        req = service.files().list(
            q=query,
            fields='nextPageToken, files(id,name,parents,createdTime)',
            supportsAllDrives=True,
            supportsTeamDrives=True,
            includeTeamDriveItems=True,
            pageToken=page_token,
            pageSize=1000
        )
        
        res = req.execute()
        
        result.extend(res['files'])

        page_token = res.get('nextPageToken', None)
        if page_token is None:
            break
    
    return result

def createFolder(service, folderName, parentFolderId):
  fileMetadata = {
    'name': folderName,
    'mimeType': 'application/vnd.google-apps.folder',
    'parents': [parentFolderId]
  }
  file = service.files().create(
    body = fileMetadata,
    supportsAllDrives = True,
    fields = 'id'
  ).execute()
    
  return file['id']

def createMissingFolders(service, rootFolderId, pathComponents):
  currentFolderId = rootFolderId
  
  for component in pathComponents:
    existingFolderId = findFolderInFolder(service, currentFolderId, component)
        
    if existingFolderId is None:
      newFolderId = createFolder(service, component, currentFolderId)
      currentFolderId = newFolderId
    else:
      currentFolderId = existingFolderId

  return currentFolderId

def uploadFileWithProgress(service, filePath, parentFolderId):
  fileName = os.path.basename(filePath)
  media = MediaFileUpload(filePath, resumable=True)
  
  fileMetadata = {
    'name': fileName,
    'parents': [parentFolderId]
  }

  request = service.files().create(
    body = fileMetadata,
    media_body = media,
    fields = 'id,size',
    supportsAllDrives = True
  )

  with tqdm(
    desc = fileName,
    total = os.path.getsize(filePath),
    unit = 'B',
    unit_scale = True,
    unit_divisor = 1024
  ) as bar:
      response = None
      while response is None:
        status, response = request.next_chunk()
        if status:
          bar.update(status.total_size - bar.n)

  return response

def deleteLocalFile(filePath):
  try:
    os.remove(filePath)
  except Exception as e:
    print("An error occurred:", str(e))

def getFather(service, id):
  req = service.files().get(
    fileId=id,
    supportsAllDrives = True,
    supportsTeamDrives = True,
    fields='parents,name,id,mimeType'
  )

  res = req.execute()

  return res
    
def haveFileInFolder(service, fileName, mimeType, folderId):  
  query = f"'{folderId}' in parents and mimeType='{mimeType}' and name='{fileName}' and trashed=false"
    
  file = service.files().list(
    q = query,
    fields = 'files(id,name,parents)',
    supportsAllDrives = True,
    supportsTeamDrives = True,
    includeTeamDriveItems = True,
  ).execute()
    
  if  len(file) == 0: return True
  
  return False

def setInterval(func, interval):
  def wrapper():
    while True:
      func()
      time.sleep(interval)
  
  thread = threading.Thread(target=wrapper)
  thread.daemon = True
  thread.start()
  return thread

def getCurrentTimeNow():
  now = datetime.now()
  formattedTimestamp = now.strftime('%Y-%m-%dT%H:%M:%S')
  return f'[LASTSYNC]{formattedTimestamp}'
  
def writeOutput(arquivo, conteudo):
  data_hora_atual = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  conteudo_formatado = f"[{data_hora_atual}] {conteudo}"
    
  with open(arquivo, 'a') as file:
    file.seek(0, 2)
    if file.tell() > 0:
      file.seek(file.tell() - 1, 0)
      last_char = file.read(1)
      if last_char != '\n':
        file.write('\n')
    file.write(conteudo_formatado + '\n')

def listParents(service, id):
  res = service.parents().list()

  return res