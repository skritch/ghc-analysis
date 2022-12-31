from typing import Generator, Iterable
import requests
from requests_oauthlib import OAuth1
import os
import dateparser

class HereAPI:
    # I set all this OAuth up but now I don't think I even need it?
    # Can't tell.
    client_id = os.getenv('HERE_ACCESS_KEY_ID')
    client_secret = os.getenv('HERE_ACCESS_KEY_SECRET')
    oauth_url = "https://account.api.here.com/oauth2/token"
    access_token = os.getenv('HERE_API_ACCESS_TOKEN')

    api_key = os.getenv('HERE_API_KEY')

    transit_route_url = "https://transit.router.hereapi.com/v8/routes"
    matrix_url = "https://matrix.router.hereapi.com/v8/matrix"
    geocode_url = 'https://geocode.search.hereapi.com/v1/geocode'

def get_access_token():
    """
    Based on https://developer.here.com/documentation/geocoding-search-api/dev_guide/topics/get-credentials-ols.html
    But had to change `grantType` to `grant_type` and change how json was sent.
    What is going on over there? 8-)

    The other guide at https://developer.here.com/documentation/identity-access-management/dev_guide/topics/sdk.html
    didn't work at all.
    """

    data = {
        'grant_type': 'client_credentials',
        'clientId': HereAPI.client_id,  
        'clientSecret': HereAPI.client_secret
    }
    response = requests.post(
        url=HereAPI.oauth_url,
        auth=OAuth1(HereAPI.client_id, client_secret=HereAPI.client_secret),
        data=data
    )
    return response.json()['access_token']

def get_oauth_session():
    if HereAPI.access_token:
        access_token = HereAPI.access_token
    else:
        access_token = get_access_token()
    
    s = requests.session()
    s.headers.update({'Authorization': f'bearer {access_token}'})
    return s

def get_apikey_session():
    s = requests.session()
    s.params['apiKey'] = HereAPI.api_key
    return s


def geocode_addresses(addresses: Iterable[str]) -> Generator[dict | None, None, None]:
    r = get_oauth_session()
    # https://developer.here.com/documentation/geocoding-search-api/dev_guide/topics/quick-start.html
    for a in addresses:
        response = r.get(HereAPI.geocode_url, params={'q': a})
        j = response.json()
        yield j['items'][0] if j['items'] else None


def get_transit_travel_time(pairs: list[tuple[tuple[float, float], tuple[float, float]]]):
    """
    [((lat0, lon0), (lat1, lon1))]
    https://developer.here.com/documentation/public-transit/dev_guide/routing/route-example.html
    """
    r = get_oauth_session()

    for p in pairs:
        response = r.get(HereAPI.transit_route_url, 
            params={'origin': f'{p[0][0]},{p[0][1]}', 'destination': f'{p[1][0]},{p[1][1]}'}
        )
        d = response.json()
        if d['routes']:
            route = d['routes'][0]
            sections = route['sections']
            departure = dateparser.parse(sections[0]['departure']['time'])
            arrival = dateparser.parse(sections[-1]['arrival']['time'])
            dt = (arrival - departure).seconds
            yield dt

        else:
            yield None


def get_travel_time_matrix(locs: list[tuple[float, float]]):
    """
    [((lat0, lon0), (lat1, lon1))]
    https://developer.here.com/documentation/public-transit/dev_guide/routing/route-example.html
    """
    r = get_oauth_session()

    latlons = [{'lat': loc[0], 'lng': loc[1]} for loc in locs]
    request_data = {
        'origins': latlons,
        'destinations': latlons,
        'regionDefinition': {'type': 'autoCircle'}
    }

    response = r.post(
        HereAPI.matrix_url, params={'async': 'false'}, 
        json=request_data
    )
    try:
        response.raise_for_status()
        matrix_data = response.json()['matrix']
        # entry k corresponds to origin i and destination j by the formula:
        # k = num_destinations * i + j
        n = len(latlons)
        travel_times = [
            (k // n, k % n, t)
            for (k, t) in enumerate(matrix_data['travelTimes'])
        ]
    except:
        print(response.text)
        raise
    return travel_times







def test_oauth():
    search_query = 'https://discover.search.hereapi.com/v1/discover?at=52.521,13.3807&q=berlin'

    r = get_oauth_session()
    print(r.get(search_query).status_code)

if __name__ == '__main__':
    test_oauth()