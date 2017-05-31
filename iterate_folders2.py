# -*- coding: utf-8 -*-
import os
import xml.etree.ElementTree as etree
import psycopg2

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

def read_xml(xml_path):
    tree = etree.parse(xml_path)
    return tree   

def get_title(tree):
    a_title = ''
    for title in tree.iterfind('front/article-meta/title-group/article-title'):
        a_title = etree.tostring(title, method='text', short_empty_elements=False, encoding="unicode")
    a_title = a_title.replace("'", "").replace(u'\n', '')
    return a_title

def get_pmid(tree):
    a_pmid = ''
    for pmid in tree.iterfind('front/article-meta/article-id[@pub-id-type="pmid"]'):
        a_pmid = pmid.text
    return a_pmid

def get_pmc(tree):
    a_pmc = ''
    for pmc in tree.iterfind('front/article-meta/article-id[@pub-id-type="pmc"]'):
        a_pmc = pmc.text
    return a_pmc

def get_authors(tree):
    for name in tree.iterfind('front/article-meta/contrib-group/contrib/name'):
        a_surname = name.find('surname').text
        a_name = name.find('given-names').text

def get_epub_date(tree):
    a_epub_date = ''
    for epub_date in tree.iterfind('front/article-meta/pub-date[@pub-type="epub"]'):
        if epub_date.find('day') == None:
            a_epub_day = '1'
        else:
            a_epub_day = epub_date.find('day').text
        if epub_date.find('month') == None or epub_date.find('year') == None:
            a_epub_date = '1900-01-01'
        else:
            a_epub_month = epub_date.find('month').text
            a_epub_year = epub_date.find('year').text
            if int(a_epub_day) < 10 and len(a_epub_day) < 2:
                a_epub_day = '0' + a_epub_day

            if int(a_epub_month) < 10 and len(a_epub_month) < 2:
                a_epub_month = '0' + a_epub_month
        
            a_epub_date = (a_epub_year + '-' + a_epub_month + '-' + a_epub_day).replace(u'\n', '')

    return a_epub_date

def get_epub_date(tree):
    a_epub_date = ''
    for epub_date in tree.iterfind('front/article-meta/pub-date[@pub-type="epub"]'):
        if epub_date.find('day') == None:
            a_epub_day = '1'
        else:
            a_epub_day = epub_date.find('day').text
        if epub_date.find('month') == None or epub_date.find('year') == None:
            a_epub_date = '1900-01-01'
        else:
            a_epub_month = epub_date.find('month').text
            a_epub_year = epub_date.find('year').text
            if int(a_epub_day) < 10 and len(a_epub_day) < 2:
                a_epub_day = '0' + a_epub_day

            if int(a_epub_month) < 10 and len(a_epub_month) < 2:
                a_epub_month = '0' + a_epub_month
        
            a_epub_date = (a_epub_year + '-' + a_epub_month + '-' + a_epub_day).replace(u'\n', '')

    return a_epub_date

def get_pmc_year(tree):
    a_pmc_year = ''
    for pmc_release in tree.iterfind('front/article-meta/pub-date[@pub-type="pmc-release"]'):
        a_pmc_year = pmc_release.find('year')
        if a_pmc_year == None:
            a_pmc_year = ''
        else:
            a_pmc_year = pmc_release.find('year').text.strip()

    return a_pmc_year

def get_pmc_month(tree):
    a_pmc_month = ''
    for pmc_release in tree.iterfind('front/article-meta/pub-date[@pub-type="pmc-release"]'):
        a_pmc_month = pmc_release.find('month')
        if a_pmc_month == None:
            a_pmc_month = ''
        else:
            a_pmc_month = pmc_release.find('month').text.strip()

    return a_pmc_month

def get_pmc_day(tree):
    a_pmc_day = ''
    for pmc_release in tree.iterfind('front/article-meta/pub-date[@pub-type="pmc-release"]'):
        a_pmc_day = pmc_release.find('day')
        if a_pmc_day == None:
            a_pmc_day = ''
        else:
            a_pmc_day = pmc_release.find('day').text.strip()

    return a_pmc_day

def get_epub_year(tree):
    a_epub_year = ''
    for epub_date in tree.iterfind('front/article-meta/pub-date[@pub-type="epub"]'):
        a_epub_year = epub_date.find('year')
        if a_epub_year == None:
            a_epub_year = ''
        else:
            a_epub_year = epub_date.find('year').text.strip()
        
    return a_epub_year

def get_epub_month(tree):
    a_epub_month = ''
    for epub_date in tree.iterfind('front/article-meta/pub-date[@pub-type="epub"]'):
        a_epub_month = epub_date.find('month')
        if a_epub_month == None:
            a_epub_month = ''
        else:
            a_epub_month = epub_date.find('month').text.strip()
        
    return a_epub_month

def get_epub_day(tree):
    a_epub_day = ''
    for epub_date in tree.iterfind('front/article-meta/pub-date[@pub-type="epub"]'):
        a_epub_day = epub_date.find('day')
        if a_epub_day == None:
            a_epub_day = ''
        else:
            a_epub_day = epub_date.find('day').text.strip()
        
    return a_epub_day

def get_abstract(tree):
    a_abstract = ''
    root = tree.getroot()
    
    for abstract in root.iter('abstract'):
        a_abstract = etree.tostring(abstract, method='text', short_empty_elements=False, encoding="unicode")
    a_abstract = a_abstract.replace("'", "").replace(u'\n', '')

    return a_abstract    

def get_body(tree):
    a_body = ''
    root = tree.getroot()
    
    for body in root.iter('body'):
        a_body = etree.tostring(body, method='text', short_empty_elements=False, encoding="unicode")
    a_body = a_body.replace("'", "").replace(u'\n', '')

    return a_body

def insert_into_db(a_pmid, a_pmc, a_epub_date, a_pmc_date, a_abstract, a_body, a_title, a_epub_year, a_epub_month, a_epub_day, a_pmc_year, a_pmc_month, a_pmc_day):

    sql = 'insert into hgd."PubMedArticle"("PMID", "PMC_ID", "TYPE", "EPUB_RELEASED", "PMC_RELEASED", "ABSTRACT", "ARTICLE_TXT", "TITLE", "EPUB_YEAR", "EPUB_MONTH", "EPUB_DAY", "PMC_YEAR", "PMC_MONTH", "PMC_DAY") values (' + a_pmid + ',' + a_pmc + ',1,' + a_epub_date + ',' + a_pmc_date + ',\'' + a_abstract + '\',\'' + a_body + '\',\'' + a_title + '\',' + a_epub_year + ',' + a_epub_month + ',' + a_epub_day + ',' + a_pmc_year + ',' + a_pmc_month + ',' + a_pmc_day + ')'

    cur = conn.cursor()

    try:
        cur.execute(sql)
        print('Article inserted.\n')
    except:
        print('Cannot insert!!! :(')
        error = '/home/emi/Documents/PubMed/ERROR/error' + str(a_pmid) + '.txt'
        error_file = open(error, 'w+',  encoding='utf-8')
        error_file.writelines(sql)

def main():
    p = '/home/emi/Documents/PubMed/Articles'

    if_folder(p)
    
    for file in FILES:
        tree = read_xml(file)
        pmid = get_pmid(tree)
        if pmid == '':
            pmid = 'NULL'
        pmc = get_pmc(tree)
        if pmc == '':
            pmc = 'NULL'
        epub_year = get_epub_year(tree)
        if epub_year == '':
            epub_year = 'NULL'
        epub_month = get_epub_month(tree)
        if epub_month == '':
            epub_month = 'NULL'
        epub_day = get_epub_day(tree)
        if epub_day == '':
            epub_day = 'NULL'
        pmc_year = get_pmc_year(tree)
        if pmc_year == '':
            pmc_year = 'NULL'
        pmc_month = get_pmc_month(tree)
        if pmc_month == '':
            pmc_month = 'NULL'
        pmc_day = get_pmc_day(tree)
        if pmc_day == '':
            pmc_day = 'NULL'

        if epub_year == 'NULL' or epub_month == 'NULL' or epub_day == 'NULL':
            epub_date = 'NULL'
        else:
            epub_date = '\'' + epub_year + '-' + ('0' + epub_month)[-2:] + '-' + ('0' + epub_day)[-2:] + '\''
            

        if pmc_year == 'NULL' or pmc_month == 'NULL' or pmc_day == 'NULL':
            pmc_date = 'NULL'
        else:
            pmc_date = '\'' + pmc_year + '-' + ('0' + pmc_month)[-2:] + '-' + ('0' + pmc_day)[-2:] + '\''
            
        abstract = get_abstract(tree)
        body = get_body(tree)
        title = get_title(tree)
        
        print('Processing ' + str(file))
        insert_into_db(pmid, pmc, epub_date, pmc_date, abstract, body, title, epub_year, epub_month, epub_day, pmc_year, pmc_month, pmc_day)

if __name__ == '__main__':
    main()
    
