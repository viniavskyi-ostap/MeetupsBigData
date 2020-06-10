import os
import yaml

from flask import Blueprint, jsonify, abort
from dse.cluster import Cluster
from dse.auth import PlainTextAuthProvider
stream_route = Blueprint('stream', __name__)

with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as fd:
    config = yaml.full_load(fd)

auth_provider = PlainTextAuthProvider(config['CASSANDRA_USER'], config['CASSANDRA_PASSWORD'])
cluster = Cluster(config['CASSANDRA_HOSTS'], auth_provider=auth_provider)


@stream_route.route('/countries')
def get_countries():
    session = cluster.connect()
    rows = session.execute('select distinct country from meetups.event_cities')
    return jsonify([r.country for r in rows])


@stream_route.route('/cities/<country_code>')
def get_cities_by_coutry(country_code):
    session = cluster.connect()
    rows = session.execute('select city from meetups.event_cities where country = %s', (country_code,))
    res = [r.city for r in rows]

    if len(res) == 0:
        abort(404)

    return jsonify(res)


@stream_route.route('/events/<event_id>')
def get_event_by_id(event_id):
    names = ['event_name', 'event_time', 'topics', 'group_name', 'country', 'city']
    session = cluster.connect()
    row = next(iter(session.execute("select * from meetups.events where event_id = %s", (str(event_id),))), None)

    if row is None:
        abort(404)

    res = {name: getattr(row, name) for name in names}
    res['topics'] = res['topics'].split(';')
    return jsonify(res)


@stream_route.route('/groups/<city_name>')
def get_groups_by_city(city_name):
    names = ['city_name', 'group_name', 'group_id']
    session = cluster.connect()
    rows = session.execute('select * from meetups.cities_groups where city_name = %s', (city_name,))

    res = [
        {name: getattr(row, name) for name in names} for row in rows
    ]

    if len(res) == 0:
        abort(404)

    return jsonify(res)


@stream_route.route('/events_by_group/<int:group_id>')
def get_event_by_group(group_id):
    names = ['event_name', 'event_time', 'topics', 'group_name', 'country', 'city']
    session = cluster.connect()
    rows = session.execute('select * from meetups.groups_events where group_id = %s', (group_id,))

    res = [
        {name: getattr(row, name) for name in names} for row in rows
    ]

    if len(res) == 0:
        abort(404)

    return jsonify(res)
