#!/usr/bin/env python
# coding: utf-8

import nltk
import psycopg2
import os
import csv

from nltk.tokenize import WordPunctTokenizer
from nltk.tokenize.treebank import TreebankWordDetokenizer


def pre_process(text: str) -> str:
    """Cast to lower and remove punctuation."""
    punctuation = '!"#$&\'()*+-:;<=>?@[\\]^_`{|}~1234567890.'
    text = text.lower()
    nopunct_text = "".join([char for char in text if char not in punctuation])
    return nopunct_text


def tokenize_and_remove_stopwords(text: str) -> list[str]:
    stopword = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 
        'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 
        'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 
        'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
         'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are',
         'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing',
         'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 
        'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 
        'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 
        'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 
        'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 
        's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y',
         'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 
        'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn',
         "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 
        'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't", "indications", "usage", "1", 
        "use", "used", "directed", "nan", "direction", "help", "reduce", "prevent", "cause", "potentially", "us", 
        "may", "see", "clinical", "indicated", "treatment", "treat", "treated", "study", "tablet", 
        "years", "age", "patients", "year", "patient", "usp", "temporarily", "relief", "caused", "older", 
        "information", "temporary", "hour", "approved", "uses", "relieved", "relieve", "symptoms", "symptom", "associated", 
        "controlled", "control", "trial", "trials", "trialed", "helps", "helped", "help", 
        "extended", "release", "tablet", "available", "measure", "measures", "measured", "direction", "directions", 
        "adult", "soap", "water", "physician", "physicians", "factor", "factors", "dosage administration", "absolute", 
        "structure", "structures", "structured", "adjunctive therapy", "adjuvant therapy", "data", "local", "would", "expect", "expects", 
        "expected", "wide", "variety", "varieties", "also", "seen", "saw", "make", "made"
    ]
    t = WordPunctTokenizer().tokenize(text)
    text = [word for word in t if word not in stopword]
    return t


def lemmatize_detokenize(tokenized_text: list[str]) -> str:
    lemmatizer = nltk.WordNetLemmatizer()
    text = [lemmatizer.lemmatize(word) for word in tokenized_text]
    text = TreebankWordDetokenizer().detokenize(text)
    return text


if __name__ == "__main__":
    # Connect to research database and run query to get product label ID & field to run text processing on
    conn = psycopg2.connect(
        dbname="public_datasets",
        user=os.environ["AWS_DEV_POSTGRES_DB_USER"],
        password=os.environ["AWS_DEV_POSTGRES_DB_PASSWORD"],
        host=os.environ["AWS_DEV_POSTGRES_DB_HOST"],
        port=os.environ["AWS_DEV_POSTGRES_DB_PORT"],
    )
    query = """
        SELECT 
            id AS product_labels_id,
            indications_and_usage
        FROM bronze.openfda_product_labels;
        ;
    """

    cur = conn.cursor(name="server_side")
    cur.execute(query)

    # Download NLTK resources in preparation for text processing
    nltk.download('wordnet')

    # Process data and write to CSV files in batches of 80,000 rows - GSheet import limit is 100 MB
    file_num = 1
    record_id = 1
    fd = None
    writer = None

    for product_label_id, indication_text in cur:
        if fd == None:
            fd = open(f"output_{file_num}.csv", "w")
            writer = csv.writer(fd)
            writer.writerow(["id", "product_label_id", "indication"])
        elif record_id % 80000 == 0:  # Skip header after first batch as we import into GSheet via append
            file_num += 1
            fd.close()
            fd = open(f"output_{file_num}.csv", "w")
            print(f"Now writing to file output_{file_num}.csv")
            writer = csv.writer(fd)
        
        processed_text = pre_process(str(indication_text))
        tokenized_text = tokenize_and_remove_stopwords(processed_text)
        output_text = lemmatize_detokenize(tokenized_text)

        if output_text == "none":
            output_text == ""
            
        writer.writerow([record_id, product_label_id, output_text.split()])
        record_id += 1
        if record_id % 20000 == 0:
            print(f"Processed {record_id} records!")

    # Clean up and close connections
    fd.close()
    cur.close()
    conn.close()
