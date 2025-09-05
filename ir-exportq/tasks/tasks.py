from celery import Celery
import celeryconfig
import json
import requests
import csv
import time
from datetime import datetime
import boto3
import os
from botocore.exceptions import ClientError

# The S3 bucket the report will be uploaded to 
S3_DESTINATION_BUCKET = os.getenv("S3_BUCKET_IR_REPORT", "cubl-ir-reports")

# The url for the Scholar app (without the ending '/')
SCHOLAR_URL = os.getenv("SCHOLAR_URL", "https://scholar.colorado.edu")

app = Celery()
app.config_from_object(celeryconfig)

workTypeDict = {
    'GraduateThesisOrDissertation': 'graduate_thesis_or_dissertations',
    'UndergraduateHonorsThesis': 'undergraduate_honors_theses',
    'Dataset': 'datasets',
    'Article': 'articles',
    'Presentation': 'presentations',
    'ConferenceProceeding': 'conference_proceedings',
    'Book': 'books',
    'BookChapter': 'book_chapters',
    'Report': 'reports',
    'Default': 'defaults'
}


def uploadToS3(countRecords):
    s3_client = boto3.client('s3')
    filePath = datetime.now().strftime('%Y-%m-%d') + '-ir-export.csv'
    if os.path.isfile(filePath):
        try:
            response = s3_client.upload_file(
                filePath, S3_DESTINATION_BUCKET, filePath)
            os.remove(filePath)
        except ClientError as e:
            return {'message': 'unable to upload to s3. Check log for more information.'}
        return {'message': 'Total: ' + str(countRecords) + ' records has been exported. File: ' + filePath + ' has been uploaded to S3.'}
    else:
        return {'message': 'File is not exits'}


@app.task()
def runExport():
    #url = 'https://scholar.colorado.edu/catalog.json?per_page=100&q=&search_field=all_fields'
    url = SCHOLAR_URL + '/catalog.json?per_page=100&q=&search_field=all_fields'
    initPage = url + '&page=1'
    response = requests.get(initPage)
    print(response.status_code)
    print(response.text)
    total_pages = response.json()['response']['pages']['total_pages']
    print(total_pages)
    fields = ['Title', 'Academic Affilation', 'Resource Type', 'URL']
    rows = []
    links = []
    fileName = datetime.now().strftime('%Y-%m-%d') + '-ir-export.csv'
    countRecords = 0
    for pageNumber in range(total_pages):
        pageNumber = pageNumber + 1
        pageURL = url + '&page=' + str(pageNumber)
        time.sleep(1)
        for doc in requests.get(pageURL).json()['response']['docs']:
            links = []
            try:
                title = ', '.join(doc['title_tesim'])
            except:
                title = 'error'
            try:
                if 'Collection' in doc['has_model_ssim']:
                    academic = ''
                else:
                    academic = ', '.join(doc['academic_affiliation_tesim'])
            except:
                academic = 'error'
            try:
                if 'Collection' in doc['has_model_ssim']:
                    resource = ''
                else:
                    resource = ', '.join(doc['resource_type_tesim'])
            except:
                resource = 'error'

            try:
                if 'Collection' in doc['has_model_ssim']:
                    #link = 'https://scholar.colorado.edu/collections/' + \
                    link = SCHOLAR_URL + '/collections/' + doc['id']
                else:
                    works = doc['has_model_ssim']
                    for work in works:
                        #links.append('https://scholar.colorado.edu/concern/' +
                        links.append(SCHOLAR_URL + '/concern/' +
                                     workTypeDict[work] + '/' + doc['id'])
                    link = ', '.join(links)
            except:
                link = 'error'
            rows.append([title, academic, resource, link])
            countRecords = countRecords + 1

    with open(fileName, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        csvwriter.writerows(rows)
    return uploadToS3(countRecords)
