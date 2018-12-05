from urllib.parse import quote_plus
from datetime import datetime, date
import dateutil.parser
import requests


class atlas_request_error(Exception):
    pass


class atlas_server_error(Exception):
    pass


class atlas_request:
    ATLAS_API_KEY = None
    ATLAS_API_BASE_URL = 'https://atlas.infegy.com/api/v2/'

    def __init__(self, query, **kwargs):
        self.query = query

        # Parse args
        for k, v in kwargs.items():
            if isinstance(k, str):
                k = k.strip()
            if isinstance(v, str):
                v = v.strip()
            if k and v:
                setattr(self, k, v)

        self.__request_cache = {}

    def __type_massage__(self, t):
        if isinstance(t, bool):
            return '1' if t else '0'
        if isinstance(t, list) or isinstance(t, tuple):
            return ','.join(str(v) for v in t)
        if isinstance(t, datetime):
            return t.date().isoformat()
        if isinstance(t, date):
            return t.isoformat()
        else:
            return str(t)

    def uri(self, endpoint):
        if self.ATLAS_API_KEY is None:
            raise atlas_request_error(
                "You must set an Atlas API key on this class before use, e.g.:"
                "from atlas_api import atlas_request\n"
                "atlas_request.ATLAS_API_KEY='YOUR_KEY_HERE'")

        uri_string = self.ATLAS_API_BASE_URL + \
            endpoint + '?api_key=' + self.ATLAS_API_KEY

        for k in [k for k in dir(self) if not callable(getattr(self, k))
                  and not k.startswith('_') and not k.isupper()]:
            v = getattr(self, k)
            if v is None:
                continue

            uri_string += '&%s=%s' % (k, quote_plus(self.__type_massage__(v)))
        return uri_string

    def url(self, endpoint):
        return self.uri(endpoint)

    def run_raw(self, endpoint, skip_cache=False):
        if not skip_cache and endpoint in self.__request_cache:
            return self.__request_cache[endpoint]

        r = requests.get(self.uri(endpoint))
        if r.status_code >= 400 and r.status_code < 500:
            raise atlas_request_error('Your request has an error (code %d): %s'
                                      % (r.status_code, r.json()['status_message']))
        elif r.status_code >= 500:
            if r.text.startswith('{'):
                raise atlas_server_error('Atlas has errored (code %d): %s'
                                         % (r.status_code, r.json()['status_message']))
            else:
                raise atlas_server_error(
                    'Atlas error (code %d): %s' % (r.status_code, r.text))

        try:
            raw_json = r.json()
        except BaseException:
            raise atlas_server_error(
                'Atlas returned something that can\'t be parsed as JSON')
        if not isinstance(raw_json, object):
            raise atlas_server_error(
                'Atlas returned something that isn\'t a JSON object')

        if 'status' not in raw_json:
            raise atlas_server_error('Atlas returned an object with no status')
        if raw_json['status'] != 'OK':
            raise atlas_server_error('Atlas a bad status with no error code. '
                                     'Status: %s, Status message: %s',
                                     raw_json['status'], raw_json.get('status_message', ''))

        self.__request_cache[endpoint] = raw_json

        return raw_json

    def run(self, endpoint, skip_cache=False):
        raw_json = self.run_raw(endpoint, skip_cache)
        return atlas_response(raw_json)

    # The various endpoint utility functions...

    for endpoint in (
            'ages',
            'brands',
            'channels',
            'countries',
            'demographics',
            'education',
            'emotions',
            'entities',
            'events',
            'gender',
            'hashtags',
            'headlines',
            'home-ownership',
            'household-value',
            'income',
            'influence-distribution',
            'influencers',
            'interests',
            'languages',
            'linguistics-stats',
            'negative-keywords',
            'negative-topics',
            'positive-keywords',
            'positive-topics',
            'post-interests',
            'posts',
            'query-test',
            'sentiment',
            'states',
            'stories',
            'themes',
            'timeofday',
            'topic-clusters',
            'topics',
            'volume',
    ):
        def add_endpoint(namespace, endpoint):
            def function(self):
                return self.run(endpoint).output
            function_name = endpoint.replace("-", "_")
            function.__name__ = function_name
            namespace[function_name]

        add_endpoint(locals(), endpoint)
        del add_endpoint
        del endpoint

    def meta(self):
        return self.run('volume').query_meta


class atlas_response(object):
    def __init__(self, obj):
        for k, v in obj.items():
            if isinstance(v, str):
                if 8 < len(v) < 22:
                    try:
                        dt = dateutil.parser.parse(v)
                        setattr(self, k, dt)
                        continue
                    except ValueError:
                        pass
                setattr(self, k, v)
            elif type(v) in (float, int, date, datetime, bool):
                setattr(self, k, v)
            elif isinstance(v, dict):
                setattr(self, k, atlas_response(v))
            else:
                setattr(self, k, [])
                for o in v:
                    if isinstance(o, dict):
                        getattr(self, k).append(atlas_response(o))
                    else:
                        getattr(self, k).append(o)
