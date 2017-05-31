#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import xml.etree.ElementTree as etree
import psycopg2
from ftplib import FTP
import os
try:
    import pandas as pd
except:
    print("Please install python-pandas:\n\t$ sudo apt-get install python-pandas")
    exit(1)

try:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='postgres'")
    conn.autocommit = True
except:
    print("I am unable to connect to the database")

FILES = []
ERROR = []

def download_gene_file(ftp_server, ftp_catalogue, ftp_filename, local_directory):

    ftp = FTP(ftp_server)
    ftp.login()
    ftp.cwd(ftp_catalogue)

    print('Downloading ' + ftp_filename)

    localfile = open(local_directory + ftp_filename, 'wb')
    ftp.retrbinary('RETR ' + ftp_filename, localfile.write, 1024)
    localfile.close()

    ftp.quit()

def get_gene_data(gene_file):
    df = pd.read_csv(gene_file, sep="\t")
    return df

def insert_genes(gene_symbol, gene_name):
    sql = 'insert into hgd."Genes"("Symbol","Name") values (\'' + gene_symbol.replace("'", "") + '\',\'' + gene_name.replace("'", "''") + '\')'

    cur = conn.cursor()

    try:
        cur.execute(sql)
    except:
        print('Cannot insert!!! :(')
        error = '/home/emi/Documents/PubMed/ERROR/gene_error' + str(a_pmid) + '.txt'
        error_file = open(error, 'w+',  encoding='utf-8')
        error_file.writelines(sql)

def insert_alias(gene_symbol, alias):
    sql = 'insert into hgd."GeneAliases"("GeneSymbol","Alias") values (\'' + gene_symbol.replace("'","") + '\',\'' + alias.replace("'","") + '\')'

    cur = conn.cursor()

    try:
        cur.execute(sql)
    except:
        print('Cannot insert!!! :(')
        error = '/home/emi/Documents/PubMed/ERROR/gene_error' + str(a_pmid) + '.txt'
        error_file = open(error, 'w+',  encoding='utf-8')
        error_file.writelines(sql)

def main():
    p = '/home/emi/Documents/PubMed/GENES/'
    url = 'ftp://ftp.ebi.ac.uk/pub/databases/genenames/new/tsv/hgnc_complete_set.txt'
    ftp_server = 'ftp.ebi.ac.uk'
    ftp_catalogue = '/pub/databases/genenames/new/tsv/'
    ftp_filename = 'hgnc_complete_set.txt'
    local_directory = '/home/emi/Documents/PubMed/GENES/'

    download_gene_file(ftp_server, ftp_catalogue, ftp_filename, local_directory)
        
    print('Processing ' + str(ftp_filename))
    genes = get_gene_data(local_directory + ftp_filename)
    genes_symbols = genes[['symbol', 'name']].copy()
    genes_alias = genes[['symbol', 'alias_symbol']].copy()
    genes_old_symbols = genes[['symbol', 'prev_symbol']].copy()

    print('Inserting genes...')
    for index, row in genes_symbols.iterrows():
        insert_genes(row['symbol'], row['name'])
        insert_alias(row['symbol'], row['symbol'])

    print('Inserting gene aliases...')
    for index, row in genes_alias.iterrows():
        if str(row['alias_symbol']).lower() != 'nan':
            for al in str(row['alias_symbol']).split('|'):
                insert_alias(row['symbol'],al)

    print('Inserting gene old symbols...')
    for index, row in genes_old_symbols.iterrows():
        if str(row['prev_symbol']).lower() != 'nan':
            for al in str(row['prev_symbol']).split('|'):
                insert_alias(row['symbol'],al)
    
if __name__ == '__main__':
    main()
    
