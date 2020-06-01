import pickle
import requests
import json
from py2neo import Database, Graph, Node, Relationship
import re
import en_core_web_sm
import os
import random
import time
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

nlp = en_core_web_sm.load()

article_uri = dict()
URL = "http://eventregistry.org/api/v1/article/getArticles"
KEYWORDS = [["Municipal Bonds", "Bond", "Municipal"], ["Finance", "Business"], ["Stock", "Equity", "Share"], ["Technology", "Tech"], ["Sports"], ["Politics"]]


def clear_graph(graph):
    print("[+] Clearing previous graph")
    # graph.cypher.execute("MATCH (A) -[R] -> () DELETE A, R")
    # graph.cypher.execute("MATCH (A) DELETE A")
    graph.delete_all()
    file_name = "Entities/entities.json"
    if os.path.isfile(file_name):
        os.remove(file_name)


def get_all_entites():
    entities = dict()

    entities["news_sources"] = dict()
    entities["authors"] = dict()
    entities["languages"] = dict()
    entities["organisations"] = dict()
    entities["locations"] = dict()
    entities["people"] = dict()
    entities["articles"] = dict()

    file_name = "Entities/entities.json"
    if os.path.isfile(file_name):
        with open(file_name) as f:
            entities = json.load(f)

    return entities


def get_articles_request(keywords, num_articles=100):
    print("[+] Getting articles for keywords:", keywords)
    articles = []

    # for keywords in KEYWORDS:
    payload = {"action": "getArticles", "keyword": keywords, "articlesPage": 1, "articlesSortBy": "date",
               "articlesSortByAsc": False, "articlesArticleBodyLen": -1, "resultType": "articles",
               "dataType": ["news", "pr"], "apiKey": "d454cb01-2c7e-4740-b4f3-01113747e508",
               "forceMaxDataTimeWindow": 31,
               "articlesCount": num_articles}

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", URL, headers=headers, data=json.dumps(payload))
    try:
        articles = json.loads(response.text.encode('utf8'))
    except:
        print("Exception occured for keyword:", keywords)
        return list()
    # print(articles.keys())
    print(type(articles))
    print("Articles request len", len(articles["articles"]["results"]))
    return articles["articles"]["results"]


def get_articles(num_articles=100):
    # curr_date = datetime.today().strftime('%d-%m-%Y')
    curr_date = "29 -05-2020"
    file_name = "Daily_Articles/" + curr_date + ".pickle"

    if not os.path.isfile(file_name):
        print("\tCould not find articles at: {file_name}, requesting".format(file_name=file_name))
        with open(file_name, "wb") as handle:
            articles = list()
            for keywords in KEYWORDS[1]:
                articles += get_articles_request(keywords, num_articles)
            articles_dict = dict()
            articles_dict["articles"] = {"results":articles}
            pickle.dump(articles_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
            return articles
    else:
        print("\tFound articles at: {file_name}".format(file_name=file_name))
        with open(file_name, 'rb') as handle:
            return pickle.load(handle)


def update_entities(entities):
    print("[+] Updating entities")

    # with open('Entities/entities.pkl', 'wb') as output:
    #     pickle.dump(entities, output, pickle.HIGHEST_PROTOCOL)

    file_name = "Entities/entities.json"
    with open(file_name, 'w') as fp:
        json.dump(entities, fp)


def get_random_number():
    return random.randint(100000000, 1000000000)


def check_wiki_page(query):
    try:
        result = requests.get('https://en.wikipedia.org/wiki/{0}'.format(query), verify=False)
    except Exception:
        pass

    if result.status_code == 200:  # the article exists
            return True
    return False


def create_graph(articles, entities, graph):
    count_ = 0
    articles_uri = entities["articles"]
    news_sources = entities["news_sources"]
    languages = entities["languages"]
    authors = entities["authors"]
    # people = entities["people"]
    # locations = entities["locations"]
    # organisations = entities["organisations"]

    new_articles = []

    print("[+] Creating nodes and relationships for languages, articles, authors and content-sources")

    for article_ in articles["articles"]["results"]:
        count_ += 1
        if article_["uri"] not in articles_uri:
            new_articles.append(article_)
            print("\tEntering data for article:", article_["uri"], "- title:", article_["title"])

            # Creating Nodes
            article_pk = get_random_number()
            article_node = Node("Article", pk=article_pk, name=article_['title'], sentiment=article_["sentiment"],
                                text=article_["body"],
                                dtype=article_["dataType"], published_on=article_["dateTimePub"], uri=article_["uri"],
                                url=article_["url"])

            articles_uri[article_["uri"]] = (article_pk, article_node)

            # Entering article
            if article_["source"]["title"] in news_sources:
                source_node_pk = news_sources[article_["source"]["title"]][0]
                query = "Match(m:`Content-Source`) where m.pk={0} Return m".format(source_node_pk)
                source_node = graph.evaluate(query)
            else:
                source_node_pk = get_random_number()
                source_node = Node("Content-Source", pk=source_node_pk, name=article_["source"]["title"],
                                   uri=article_["source"]["uri"])

            rel_article_source = Relationship(article_node, "publishedBy", source_node)
            graph.create(rel_article_source)
            news_sources[article_["source"]["title"]] = (source_node_pk, source_node)

            # Entering language
            # if article_["lang"] in languages:
            #     lang_pk = languages[article_["lang"]][0]
            #     query = "Match(m:`Language`) where m.pk={0} Return m".format(lang_pk)
            #     lang_node = graph.evaluate(query)
            # else:
            #     lang_pk = get_random_number()
            #     lang_node = Node("Language", pk=lang_pk, name=article_["lang"])
            #
            # rel_article_lang = Relationship(article_node, "language", lang_node)
            # graph.create(rel_article_lang)
            # languages[article_["lang"]] = (lang_pk, lang_node)

            # Entering authors
            for author in article_["authors"]:
                if author["name"] in authors:
                    author_pk = authors[author["name"]][0]
                    query = "Match(m:`Author`) where m.pk={0} Return m".format(author_pk)
                    author_node = graph.evaluate(query)
                else:
                    author_pk = get_random_number()
                    author_node = Node("Author", pk=author_pk, name=author["name"], uri=article_["uri"])

                rel_article_author = Relationship(article_node, "writtenBy", author_node)
                rel_author_article = Relationship(author_node, "hasWritten", article_node)

                graph.create(rel_article_author)
                graph.create(rel_author_article)

                authors[author["name"]] = (author_pk, author_node)

    print("[+] Done loading data to neo4j")

    entities["news_sources"] = news_sources
    entities["authors"] = authors
    entities["languages"] = languages
    entities["articles"] = articles_uri
    # entities["people"] = people
    # entities["locations"] = locations
    # entities["organisations"] = organisations

    # update_entities(entities)

    return entities, new_articles


def create_graph_with_parsed_entites(articles, entities, graph, valid_entities, invalid_entities):
    people = entities["people"]
    locations = entities["locations"]
    organisations = entities["organisations"]
    articles_uri = entities["articles"]
    news_sources = entities["news_sources"]
    languages = entities["languages"]
    authors = entities["authors"]

    # entity_type_map = {"ORG": "organisations", "PERSON": "people", "GPE": "location"}

    print("[+] Creating nodes and relationships for people, locations and organisations")
    print(len(articles))

    for i, article_ in enumerate(articles):
        print(i)
        start_time = time.time()

        print("\tEntering data for article:", article_["uri"], "- title:", article_["title"])

        article_pk = articles_uri[article_["uri"]][0]

        text_nlp = nlp(article_["body"])

        for ent in text_nlp.ents:

            if "%" in ent.text:
                continue

            ent_text = ent.text.lower()
            ent_text = ent_text.replace("-", "").replace("'s", "")

            if not check_wiki_page(ent_text):
                continue

            if ent.label_ is "ORG":
                if ent_text in organisations:
                    entity_pk = organisations[ent_text][0]
                    query = "Match(m:`ORGANISATION`) where m.pk={0} Return m".format(entity_pk)
                    entity_node = graph.evaluate(query)
                else:
                    entity_pk = get_random_number()
                    entity_node = Node("ORGANISATION", pk=entity_pk, name=ent_text)

                query = "Match(m:`Article`) where m.pk={0} Return m".format(article_pk)
                article_node = graph.evaluate(query)

                rel_article_org = Relationship(article_node, "referencesOrg", entity_node)
                graph.create(rel_article_org)

                organisations[ent_text] = (entity_pk, entity_node)

            elif ent.label_ is "PERSON":
                if ent_text in people:
                    entity_pk = people[ent_text][0]
                    query = "Match(m:`Person`) where m.pk={0} Return m".format(entity_pk)
                    entity_node = graph.evaluate(query)
                else:
                    entity_pk = get_random_number()
                    entity_node = Node("Person", pk=entity_pk, name=ent_text)

                query = "Match(m:`Article`) where m.pk={0} Return m".format(article_pk)
                article_node = graph.evaluate(query)

                rel_article_person = Relationship(article_node, "referencesPerson", entity_node)
                graph.create(rel_article_person)

                people[ent_text] = (entity_pk, entity_node)

            elif ent.label_ is "GPE":
                if ent_text in locations:
                    entity_pk = locations[ent_text][0]
                    query = "Match(m:`Location`) where m.pk={0} Return m".format(entity_pk)
                    entity_node = graph.evaluate(query)
                else:
                    entity_pk = get_random_number()
                    entity_node = Node("Location", pk=entity_pk, name=ent_text)

                query = "Match(m:`Article`) where m.pk={0} Return m".format(article_pk)
                article_node = graph.evaluate(query)

                rel_article_loc = Relationship(article_node, "referencesLoc", entity_node)
                graph.create(rel_article_loc)

                locations[ent_text] = (entity_pk, entity_node)
            else:
                continue
            # valid_entities.add((article_["uri"], ent.text, ent.label_))

        print("--- %s seconds ---" % (time.time() - start_time))

    entities["people"] = people
    entities["locations"] = locations
    entities["organisations"] = organisations

    return entities


if __name__ == "__main__":
    graph = Graph(password="jay")
    valid_entities, invalid_entities = dict(), dict()
    # clear_graph(graph)
    articles = get_articles()
    entities = get_all_entites()
    time.sleep(2)
    entities, new_articles = create_graph(articles, entities, graph)
    print(len(new_articles))
    entities = create_graph_with_parsed_entites(new_articles, entities, graph, valid_entities, invalid_entities)
    update_entities(entities)
