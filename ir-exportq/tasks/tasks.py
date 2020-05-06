from celery.task import task
import json
import requests
import csv
import time
from datetime import datetime
import boto3
import os
from botocore.exceptions import ClientError

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
                filePath, 'cubl-ir-reports', '/' + filePath)
            os.remove(filePath)
        except ClientError as e:
            return {'message': 'unable to upload to s3. Check log for more information.'}
        return {'message': 'Total: ' + str(countRecords) + ' records has been exported. File: ' + filePath + ' has been uploaded to S3.'}
    else:
        return {'message': 'File is not exits'}


@task()
def runExport():
    url = 'https://scholar.colorado.edu/catalog.json?per_page=100&q=&search_field=all_fields'
    initPage = url + '&page=1'
    total_pages = requests.get(initPage).json()[
        'response']['pages']['total_pages']
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
                    link = 'https://scholar.colorado.edu/collections/' + \
                        doc['id']
                else:
                    works = doc['has_model_ssim']
                    for work in works:
                        links.append('https://scholar.colorado.edu/concern/' +
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
