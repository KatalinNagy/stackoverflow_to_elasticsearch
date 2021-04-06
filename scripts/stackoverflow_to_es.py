from stackapi import StackAPI
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
from typing import List, Dict


def main():
    """Parent function to update the stackoverflow-question-items
    Elasticsearch index with the latest Stackoverflow questions
    containing the pandas tag."""

    # Get latest date of the ES index.
    latest_date = get_latest_creation_date()

    # Extract Stackoverflow questions since the latest date
    questions = export_stackoverflow_questions('creation', 'asc', ['pandas'],
                                               latest_date)

    # Process the results and write them to Elasticsearch
    items = clean_results(questions)

    write_to_es(items, 'question_id', 'stackoverflow-question-items')


def export_stackoverflow_questions(sortby: str,
                                   orderby: str,
                                   tags: List,
                                   max_date=None):
    """This function establishes connection to Stackoverflow
    and returns the questions related to a tag based on the
    sorting and ordering criteria.
    """
    site = StackAPI('stackoverflow', max_pages=300)

    # If max_date is defined, return everything since that date
    # otherwise return the earliest results
    if max_date:
        questions = site.fetch('questions',
                               sort=sortby,
                               order=orderby,
                               tagged=tags,
                               fromdate=max_date)
    else:
        questions = site.fetch('questions',
                               sort=sortby,
                               order=orderby,
                               tagged=tags)

    return questions


def questions_to_df(questions: Dict):
    """This function turns a dictionary containing Stackoverflow
    questions into a pandas data frame.
    """

    df = pd.DataFrame(questions['items'])

    df['creation_timestamp'] = df['creation_date'].copy()
    df['creation_date'] = pd.to_datetime(df['creation_date'], unit='s')

    return df


def clean_results(questions: Dict):
    """This function transforms a dictionary containing Stackoverflow
    questions into a clean dictonary by removing unwanted fields and
    assigning the correct data types."""

    items = questions['items']

    for i in items:
        del i['owner']
        i['creation_timestamp'] = i['creation_date']
        i['creation_date'] = datetime.fromtimestamp(i['creation_date'])

    return items


def write_to_es(items, id_field: str, index_name: str):
    """This function writes the new Stackoverflow questions
    into an Elasticsearch index."""

    es_client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    helpers.bulk(es_client,
                 create_elasticsearch_documents(items,
                                                id_field,
                                                index_name))


def create_elasticsearch_documents(items, id_field, index_name):
    for item in items:
        try:
            yield {
                "_index": index_name,
                "_type": "_doc",
                "_id": "{}".format(item[id_field]),
                "_source": item,
            }
        except StopIteration:
            return


def get_latest_creation_date():
    """This function connects to a local Elasticsearch
    instance and returns the maximum value of a specified
    timestamp field of a specified index."""

    client = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    query_body = {
        "aggs": {
            "latest_date": {
                "max": {
                    "field": "creation_timestamp"
                }
            }
        },
        "size": 0}

    s = Search(using=client,
               index="stackoverflow-question-items"
               ).update_from_dict(query_body)

    t = s.execute()

    int_latest_date = t.aggregations['latest_date'].value

    dt_latest_date = datetime.fromtimestamp(int_latest_date)

    return dt_latest_date


# execute main
if __name__ == "__main__":
    main()
