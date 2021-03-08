#!/bin/python3
"""
Простой web-сервер, реализующий переадресацию запросов в ElasticSearch.

author: Maxim Dunaevsky
email: dunmaksim@yandex.ru
"""

import os  # getenv
import sys  # exit

import flask
import elasticsearch as es

_URI_INDEX = "/"
_URI_API_MOVIES_INDEX = "/api/movies/"
_URI_API_MOVIES_DETAIL = "/api/movies/<string:movie_id>"

GET = "GET"

_REQUEST_DEFAULTS = {"limit": 50, "page": 1, "sort": "id", "sort_order": "asc"}

# Параметры ElasticSearch
_DEFAULT_ES_HOST = "192.168.11.128"
_DEFAULT_ES_PORT = "9200"
_ES_HOST = os.getenv("ELASTIC_SEARCH_HOST", _DEFAULT_ES_HOST)
_ES_PORT = int(os.getenv("ELASTIC_SEARCH_PORT", _DEFAULT_ES_PORT))
_ES_SETTINGS = [{"host": _ES_HOST, "port": _ES_PORT}]
_ES_FILTER_PATH = "hits.hits._source"

# Параметры Flask
_DEFAULT_FLASK_HOST = "0.0.0.0"
_DEFAULT_FLASK_PORT = "80"
_FLASK_HOST = os.getenv("FLASK_HOST", _DEFAULT_FLASK_HOST)
_FLASK_PORT = int(os.getenv("FLASK_PORT", _DEFAULT_FLASK_PORT))

app = flask.Flask(__name__)

es_connection = es.Elasticsearch(_ES_SETTINGS)


@app.route(_URI_INDEX)
def index():
    u"""Главная страница сайта."""
    return "worked"


@app.route(_URI_API_MOVIES_INDEX, methods=[GET])
def movie_list():
    u"""API для доступа к фильмам."""

    request_params = _REQUEST_DEFAULTS.copy()

    # Тут уже валидно все
    request = flask.request
    for param in request.args.keys():
        request_params[param] = request.args.get(param)

    body = {"_source": {"include": ["id", "title", "imdb_rating"]}}

    if request_params.get("search"):
        body["query"] = {
            "multi_match": {
                "query": request_params["search"],
                "fields": ["title"]
            }
        }

    params = {
        "from":
        int(request_params["limit"]) * (int(request_params["page"]) - 1),
        "size": request_params["limit"],
        "sort": [{
            request_params["sort"]: request_params["sort_order"]
        }]
    }

    search_res = es_connection.search(body=body,
                                      index="movies",
                                      params=params,
                                      filter_path=[_ES_FILTER_PATH])

    return flask.jsonify(
        [doc["_source"] for doc in search_res["hits"]["hits"]])


@app.route(_URI_API_MOVIES_DETAIL, methods=[GET])
def get_movie(movie_id):
    u"""Возвращает фильм по ID."""
    search_result = es_connection.get(index="movies", id=movie_id, ignore=404)

    if search_result["found"]:
        return flask.jsonify(search_result["_source"])

    return flask.abort(404)


if __name__ == "__main__":
    es_connection = es.Elasticsearch(_ES_SETTINGS)
    if es_connection.ping():
        es_connection.search()
        try:
            app.run(host=_FLASK_HOST, port=_FLASK_PORT)
        except:
            # По-хорошему, надо разбирать каждое возможное иключение и выдавать
            # уведомление об ошибке.
            sys.exit("Can't start Flask application.")
    else:
        sys.exit("ElasticSearch host {} is unavailable.".format(_ES_HOST))
