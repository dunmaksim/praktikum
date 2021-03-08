#!/env/python3
"""
Программа для загрузки данных о фильмах из БД SQLite в ElasticSearch.


"""

import json  # loads
import os  # getenv
import pathlib
import sqlite3
import sys  # exit

import elasticsearch

_ES_DEFAULT_HOST = '192.168.1.252'
_ES_DEFAULT_PORT = '9200'
_ES_HOST = os.getenv('ELASTICSEARCH_HOST', _ES_DEFAULT_HOST)
_ES_PORT = int(os.getenv('ELASTICSEARCH_PORT', _ES_DEFAULT_PORT))
_ES_CONFIG = dict(host=_ES_HOST, port=_ES_PORT)

_SQLITE_DATABASE = 'db.sqlite'


def simple_extract(cur, table_name: str) -> dict:
    """Извлекает сущности из указанной таблицы в виде словаря.

    cur -- курсор в базе SQLite.
    table_name -- таблица, из которой нужно выбрать данные
    """
    query = "SELECT id, name FROM {} WHERE name != 'N/A';".format(table_name)
    data = cur.execute(query)
    result_dict = {}
    for d in data:
        result_dict[d[0]] = d[1]
    return result_dict


def extract_actors(cur) -> dict:
    """Извлекает из базы словарь актёров."""
    return simple_extract(cur, "actors")


def extract_writers(cur) -> dict:
    """
    Извлекает из базы словарь сценаристов.
    """
    return simple_extract(cur, "writers")


def extract_movies_actors(cur) -> dict:
    """
    Извлекает словарь, где id каждого фильма соответствует список id актёров.
    """

    query = "SELECT movie_id, actor_id FROM movie_actors;"
    movies_actors = {}
    for row in cur.execute(query):
        movie_id = row[0]  # id фильма
        if movie_id in movies_actors:  # Такой фильм есть? Добавим актёра.
            movies_actors[movie_id].append(row[1])
        else:  # Фильма ещё нет в списке? Добавим словарь из одного элемента
            movies_actors[movie_id] = [
                row[1],
            ]
    return movies_actors


def extract_movies(cur, actors, writers, movies_actors) -> list:
    u"""Извлекает из базы информацию о фильмах."""

    query = """SELECT id,
                      genre,
                      director,
                      title,
                      plot,
                      imdb_rating,
                      writer,
                      writers
               FROM movies;"""

    raw_movies = cur.execute(query)
    movies = []
    for raw_movie in raw_movies:
        movie = {
            "id": raw_movie[0],
            "genre": raw_movie[1],
            "director": raw_movie[2],
            "title": raw_movie[3],
            "desctiption": raw_movie[4],  # plot
            "imdb_rating": raw_movie[5],
            "actors": [],
            "writers": []
    }

        # Для полей writers_names и actors_names
        actors_names = []
        writers_names = []

        # Работа со сценаристами
        writer_id = raw_movie[6]  # writer
        # Не равно Null? Значит, только один сценарист
        if writer_id is not None and writer_id != "":
            if writer_id in writers:
                writer_name = writers[writer_id]
                movie["writers"].append({"id": writer_id, "name": writer_name})
                writers_names.append(writer_name)

        else:
            # Строка с ID сценаристов
            # Убираем [ и ] в начале и конце строки, потом делим на части
            writers_str = raw_movie[7][1:-1].split(",")
            # Каждую часть превращаем в словарь
            for writer_raw in writers_str:
                writer_dict = json.loads(writer_raw)
                # Находим нужного сценариста, добавляем в список
                writer_id = writer_dict["id"]
                if writer_id in writers:
                    writer_name = writers[writer_id]
                    movie["writers"].append({
                        "id": writer_id,
                        "name": writer_name
                    })
                    writers_names.append(writer_name)

        # Работа с актёрами
        actors_ids = map(int, movies_actors[movie["id"]])  # список id актёров

        # Добавляем в поле "actors" актёров из словаря
        for actor_id in actors_ids:
            if actor_id in actors:
                actor_name = actors[actor_id]
                movie["actors"].append({"id": actor_id, "name": actor_name})
                actors_names.append(actor_name)

        # Теперь нужно заполнить поля "writers_names" и "actors_names"
        movie["writers_names"] = ", ".join(writers_names)
        movie["actors_names"] = ", ".join(actors_names)
        movies.append(movie)
    return movies


def main() -> int:
    u"""
    Основная функция приложения.
    Алгоритм работы:
    1. Подключиться к SQLite.
    2. Подключиться к ElasticSearch.
    3. Если подключение прошло успешно, извлечь данные из SQLite.
    4. Сформировать массив данных и загрузить в ElasticSearch.
    5. Закрыть подключения.
    """

    sqlite_connection = None
    sqlite_cursor = None

    sqlite_db = pathlib.Path(_SQLITE_DATABASE)
    if sqlite_db.exists() and sqlite_db.is_file():
        try:
            sqlite_connection = sqlite3.connect(_SQLITE_DATABASE)
            sqlite_cursor = sqlite_connection.cursor()
        except:
            sys.exit("SQLite database not found.")

    es_connection = elasticsearch.Elasticsearch([_ES_CONFIG])

    if es_connection.ping():
        actors = extract_actors(sqlite_cursor)
        writers = extract_writers(sqlite_cursor)
        movies_actors = extract_movies_actors(sqlite_cursor)
        movies = extract_movies(sqlite_cursor, actors, writers, movies_actors)
        elasticsearch.helpers.bulk(es_connection, movies)
    else:
        print("ElasticSearch host {} is not available.".format(_ES_HOST))

    es_connection.close()
    sqlite_connection.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
