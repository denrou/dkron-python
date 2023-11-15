import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DkronException(Exception):
    pass


class Dkron:
    def __init__(self, hosts, verify=True):
        self.raise_errors = True
        self.base_url = None
        self.verify = verify
        for host in hosts:
            try:
                self.base_url = host
                self.verify = verify
                self.get_status()
                break
            except DkronException as ex:
                self.base_url = None
            except requests.exceptions.ConnectionError as ex:
                self.base_url = None
        if not self.base_url:
            raise DkronException(f"Unhealthy hosts {hosts}.")

    def _get_url(self, path):
        return self.base_url + path

    def _get_headers(self):
        return {}

    def _process_response(self, response):
        if self.raise_errors and response.status_code not in [200, 201, 202]:
            raise DkronException(
                "Dkron API request failed to request %s with code %d, %s"
                % (response.url, response.status_code, response.text)
            )
        return response.json()

    def get_status(self):
        url = self._get_url("/v1")
        response = requests.get(
            url, headers=self._get_headers(), timeout=20, verify=self.verify
        )
        return self._process_response(response)

    def get_leader(self):
        url = self._get_url("/v1/leader")
        response = requests.get(url, headers=self._get_headers(), verify=self.verify)
        return self._process_response(response)

    def get_members(self):
        url = self._get_url("/v1/members")
        response = requests.get(url, headers=self._get_headers(), verify=self.verify)
        return self._process_response(response)

    def get_jobs(self):
        url = self._get_url("/v1/jobs")
        response = requests.get(url, headers=self._get_headers(), verify=self.verify)
        return self._process_response(response)

    def get_job(self, name):
        url = self._get_url("/v1/jobs/%s" % name)
        response = requests.get(url, headers=self._get_headers(), verify=self.verify)
        return self._process_response(response)

    def apply_job(self, data):
        url = self._get_url("/v1/jobs")
        response = requests.post(
            url, headers=self._get_headers(), json=data, verify=self.verify
        )
        return self._process_response(response)

    def run_job(self, name):
        url = self._get_url("/v1/jobs/%s" % name)
        response = requests.post(url, headers=self._get_headers(), verify=self.verify)
        return self._process_response(response)

    def delete_job(self, name):
        url = self._get_url("/v1/jobs/%s" % name)
        response = requests.delete(url, headers=self._get_headers(), verify=self.verify)
        return self._process_response(response)

    def get_executions(self, job_name):
        url = self._get_url("/v1/jobs/%s/executions" % job_name)
        response = requests.get(url, headers=self._get_headers(), verify=self.verify)
        return self._process_response(response)
