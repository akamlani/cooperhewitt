import yaml
import requests
import json
import time
import pandas as pd



class Museum(object):

    def __init__(self):
        self.url = self.create_url()
        credentials = yaml.load(open('./../config/api_cred.yml'))
        self.api_key = credentials['cooperhewitt_key']
        self.access_token = credentials['cooperhewitt_token']

    def create_url(self):
        hostname = 'api.collection.cooperhewitt.org'
        endpoint = '/rest'
        url = "https://" + hostname + endpoint + '/'
        return url

    def _execute(self, method, args):
        # base method to execute a single request, limited to 500 records per page
        now  = int(time.time())
        args['timestamp'] = now
        args['method'] = method
        args['access_token'] = self.access_token
        args['per_page'] = 500
        response = requests.get(self.url, params=args)
        if response.status_code == 200:
            rsp_json = json.loads(response.text)
            print "Page Response={0}, Num Records={1}, Pages={2}".format(response.status_code, rsp_json['total'], rsp_json['pages'])
        else:
            print "Page Response={0}".format(response.status_code)
        return response

    def _access_records(self, rsp_json, kf, format, state="initial"):
        # handle the type of format conversion required
        if format == "dataframe":
            return pd.io.json.json_normalize(rsp_json)[kf][0]
        elif format == "json":
            if state == "initial": return rsp_json
            else: return resp_json[kf][0]


    def _execute_pages(self, method, args, kf, format="dataframe"):
        # execute successive pages via pagination
        response = self._execute(method, args)
        if(response.status_code == 200):
            rsp_json = json.loads(response.text)
            records  = self._access_records(rsp_json, kf, format)
            print "Initial Page Request: Response={0}, Num Records={1}, Args={2}" \
                .format(response.status_code, rsp_json['total'], args['tag_id'] if 'tag_id' in args else {})
        else:
            print "Initial Page Request: Response={0}, Args={1}" \
                .format(response.status_code, args['tag_id'] if 'tag_id' in args else {})
            return pd.DataFrame()

        if( (rsp_json['stat'] == u'ok') and (int(rsp_json['pages']) > 1) ):
            max_records  = int(rsp_json[u'total'])
            max_pages    = int(rsp_json[u'pages'])
            for page_id in range(2, max_pages+1):
                args['page'] = page_id
                response = self._execute(method, args)
                rsp_json = json.loads(response.text)
                if( (rsp_json['stat'] == u'ok') and (page_id <= int(rsp_json['pages'])) ):
                    records += self._access_records(rsp_json, kf, format, state="pagination")
            print "Page Results Found: {0}, Current Num Records: {1}".format(len(records) == max_records, len(records))
        return (pd.DataFrame(records) if format == "dataframe" else records)



#### begin specific site information about the museum
    def site_information(self):
        req_method = 'cooperhewitt.objects.locations.sites.getList'
        rsp = self._execute(req_method, {})
        rsp_json = json.loads(rsp.text)
        if rsp_json['stat'] == 'ok':
            metric_names = { 'id', 'name', 'count_objects', 'count_rooms', 'count_spots' }
            metrics_json = { key:value for key,value in rsp_json['sites']['site'][0].items() if key in metric_names }
            self.site_id = metrics_json['id']
            return metrics_json
        return rsp_json

    def site_objects(self):
        req_method = 'cooperhewitt.objects.locations.sites.getObjects'
        rsp_json   = self.execute_pages(req_method, {'site_id': int(self.site_id)}, u'objects')
        return rsp_json

    def site_departments(self):
        req_method = 'cooperhewitt.departments.getList'
        rsp = self._execute(req_method, {})
        rsp_json = json.loads(rsp.text)
        self.df_depts = pd.DataFrame(rsp_json['departments'])
        return self.df_depts

    def site_exhibitions(self):
        req_method = 'cooperhewitt.exhibitions.getList'
        rsp = self._execute(req_method, {})
        rsp_json = json.loads(rsp.text)
        print "Response={0}, Num Records={1}".format(rsp.status_code, rsp_json['total'])
        df_exhibitions = pd.DataFrame(rsp_json['exhibitions'])
        return df_exhibitions

    def site_objects_via_exhibition(self, exhibition_id, kw):
        records = {}
        req_method = 'cooperhewitt.exhibitions.getObjects'
        rsp_json = self._execute_pages(req_method, {'exhibition_id': exhibition_id}, u'objects', format='json')
        if rsp_json.get(kw):
            records["_id"] = str(exhibition_id)
            records[kw] = rsp_json[kw]
        return records

    def site_rooms(self):
        req_method = 'cooperhewitt.objects.locations.rooms.getList'
        rsp = self._execute(req_method, {})
        rsp_json = json.loads(rsp.text)
        num_rooms = rsp_json['total']
        df_rooms = pd.DataFrame(rsp_json['rooms']['room'])
        self.df_room_info = df_rooms[['id', 'name', 'floor', 'count_objects', 'count_spots']]
        return self.df_room_info

    def site_spots(self):
        req_method = 'cooperhewitt.objects.locations.spots.getList'
        rsp_json = self._execute_pages(req_method, {}, u'spots.spot')
        return rsp_json


    def site_types(self):
        req_method = 'cooperhewitt.types.getList'
        df_types = self._execute_pages(req_method, {}, u'types')
        return df_types
