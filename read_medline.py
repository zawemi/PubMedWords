#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import xml.etree.ElementTree as etree
import psycopg2
from ftplib import FTP
import gzip

try:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='postgres'")
    conn.autocommit = True
except:
    print("I am unable to connect to the database")

FILES = []
ERROR = []

def if_folder(path):

    for entry in os.scandir(path):
        if entry.is_dir():
            if_folder(entry.path)
        elif entry.is_file():
            FILES.append(entry.path)

    FILES.sort(reverse=True)

def read_xml(xml_path):
    tree = etree.parse(xml_path)
    return tree

def download_file(ftp_server, ftp_catalogue, local_directory):
    ftp = FTP(ftp_server)
    ftp.login()
    ftp.cwd(ftp_catalogue)
    med_files = ftp.nlst() 

    for filename in med_files:
        print('Downloading ' + filename)

        if filename.endswith(".xml.gz"):
            localfile = open(local_directory + filename, 'wb')
            ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
            localfile.close()

    ftp.quit()

def unzip_file(file, local_directory):
    f = gzip.GzipFile(file, 'rb')
    file_content = f.read()
    f.close()

    unpacked_file = file.replace(".xml.gz",".xml")
    outF = open(unpacked_file, 'wb')
    outF.write(file_content)
    outF.close()

    return unpacked_file
    
def delete_file(filename):
    os.remove(filename)

def get_article(tree):
    for name in tree.iterfind('PubmedArticle/MedlineCitation'):
        a_pmid = name.find('PMID').text
        #print(a_pmid)
        a_title = '\'' + str(name.find('Article/ArticleTitle').text).replace("'", "''").replace(u'\n', '') + '\''
        #print(a_title)
        a_abstract = name.find('Article/Abstract')
        a_abstr = ''
        if a_abstract == None:
            a_abstr = 'NULL'
        else:
            for a_abstract_part in a_abstract.iterfind('AbstractText'):
                a_abstr = a_abstr + str(etree.tostring(a_abstract_part, method='text', short_empty_elements=False, encoding="unicode" ))
            a_abstr = '\'' + str(a_abstr).replace("'", "''").replace(u'\n', '') + '\''
            #print(a_abstr)
        a_pub_year = name.find('Article/Journal/JournalIssue/PubDate/Year')
        if a_pub_year == None:
            a_pub_year = 'NULL'
        else:
            a_pub_year = name.find('Article/Journal/JournalIssue/PubDate/Year').text
        #print(a_pub_year)
        a_pub_month = name.find('Article/Journal/JournalIssue/PubDate/Month')
        if a_pub_month == None:
            a_pub_month = 'NULL'
        else:
            a_pub_month = name.find('Article/Journal/JournalIssue/PubDate/Month').text
            if a_pub_month == 'Jan':
                a_pub_month = '01'
            elif a_pub_month == 'Feb':
                a_pub_month = '02'
            elif a_pub_month == 'Mar':
                a_pub_month = '03'
            elif a_pub_month == 'Apr':
                a_pub_month = '04'
            elif a_pub_month == 'May':
                a_pub_month = '05'
            elif a_pub_month == 'Jun':
                a_pub_month = '06'
            elif a_pub_month == 'Jul':
                a_pub_month = '07'
            elif a_pub_month == 'Aug':
                a_pub_month = '08'
            elif a_pub_month == 'Sep':
                a_pub_month = '09'
            elif a_pub_month == 'Oct':
                a_pub_month = '10'
            elif a_pub_month == 'Nov':
                a_pub_month = '11'
            elif a_pub_month == 'Dec':
                a_pub_month = '12'
        #print(a_pub_month)

        a_pub_day = name.find('Article/Journal/JournalIssue/PubDate/Day')
        if a_pub_day == None:
            a_pub_day = 'NULL'
        else:
            a_pub_day = name.find('Article/Journal/JournalIssue/PubDate/Day').text
        #print(a_pub_day)

        if a_pub_year != 'NULL' and a_pub_month != 'NULL' and a_pub_day != 'NULL':
            a_pub_date = '\'' + a_pub_year + '-' + ('0' + a_pub_month)[-2:] + '-' + ('0' + a_pub_day)[-2:] + '\''
        else:
            a_pub_date = 'NULL'

        if a_pub_date[5:11] == '-04-31':
            a_pub_date = '\'' + a_pub_date[1:5]+'-04-30' + '\''
        elif a_pub_date[5:11] == '-06-31':
            a_pub_date = '\'' + a_pub_date[1:5]+'-06-30' + '\''
        elif a_pub_date[5:11] == '-09-31':
            a_pub_date = '\'' + a_pub_date[1:5]+'-09-30' + '\''
        elif a_pub_date[5:11] == '-11-31':
            a_pub_date = '\'' + a_pub_date[1:5]+'-11-30' + '\''

        a_journal = '\'' + str(name.find('MedlineJournalInfo/NlmUniqueID').text) + '\''
        #print(a_journal)

        insert_into_db(a_pmid, a_pub_date, a_journal, a_abstr, a_title, a_pub_year, a_pub_month, a_pub_day)
        
        a_mesh = name.find('MeshHeadingList')
        if a_mesh != None:
            for a_mesh_head in a_mesh.iterfind('MeshHeading'):
                a_mesh_name = a_mesh_head.find('DescriptorName').text
                #print(a_mesh_name)

def insert_into_db(a_pmid, a_pub_date, a_journal, a_abstract, a_title, a_pub_year, a_pub_month, a_pub_day):

    sql = 'insert into hgd."MedlineCitation"("PMID", "PUB_DATE", "JOURNAL", "ABSTRACT", "TITLE", "PUB_YEAR", "PUB_MONTH", "PUB_DAY") values (' + a_pmid + ',' + a_pub_date + ',' + a_journal + ',' + a_abstract + ',' + a_title + ',' + a_pub_year + ',' + a_pub_month + ',' + a_pub_day + ')'

    cur = conn.cursor()

    try:
        cur.execute(sql)
        #print('Citation inserted.\n')
    except:
        #print('Cannot insert!!! :(')
        error = '/home/emi/Documents/PubMed/ERROR/error' + str(a_pmid) + '.txt'
        error_file = open(error, 'w+',  encoding='utf-8')
        error_file.writelines(sql)

def main():
    p = '/home/emi/Documents/PubMed/MEDLINE/'
    ftp_server = 'ftp.ncbi.nlm.nih.gov'
    ftp_catalogue = '/pubmed/baseline/'

    download_file(ftp_server, ftp_catalogue, p)
    
    if_folder(p)
    
    for file in FILES:
        if file.endswith(".xml.gz"):
            print('Processing ' + str(file))
            unzipped_file = unzip_file(file, p)
            tree = read_xml(unzipped_file)
            get_article(tree)
            delete_file(unzipped_file)
                    
if __name__ == '__main__':
    main()
    
