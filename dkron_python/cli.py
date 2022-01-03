import json
import os
from typing import List, Optional
from dkron_python.api import Dkron, DkronException
import typer

app = typer.Typer()
get = typer.Typer(help="Fetch information about a resource")
apply = typer.Typer(help="Apply a resource")
delete = typer.Typer(help="Delete a resource")
app.add_typer(get, name="get")
app.add_typer(apply, name="apply")
app.add_typer(delete, name="delete")

_DKRON_ENV_NAME_HOSTS = "DKRON_HOSTS"
api = None

HostsOption = typer.Option(
    None,
    "-h",
    "--hosts",
    help="Dkron instance URLs, separated with commas",
    envvar=_DKRON_ENV_NAME_HOSTS,
)
InsecureOption = typer.Option(
    False,
    "-k",
    "--insecure",
    help="Allow insecure connections when using SSL",
    is_flag=True,
)
JobName = typer.Argument(..., help="Name of the job")


@app.callback()
def cli(hosts: Optional[str] = HostsOption, insecure: bool = InsecureOption):
    """
    Command line interface client for Dkron
    """
    global api
    if not hosts:
        print(
            f"You must provide {_DKRON_ENV_NAME_HOSTS} environment variable OR --hosts option."
        )
        print("Check docs: https://github.com/centreon/dkron-python#cli-usage")
        exit(1)
    api = Dkron(hosts.split(","), verify=not insecure)


@get.command()
def status():
    """
    Get system status
    """
    try:
        results = api.get_status()
    except DkronException as ex:
        print("Error while fetching: %s" % str(ex))
        exit(1)
    print(json.dumps(results))


@get.command()
def leader():
    """
    Get system leader
    """
    try:
        results = api.get_leader()
    except DkronException as ex:
        print("Error while fetching: %s" % str(ex))
        exit(1)
    print(json.dumps(results))


@get.command()
def members():
    """
    Get system members
    """
    try:
        results = api.get_members()
    except DkronException as ex:
        print("Error while fetching: %s" % str(ex))
        exit(1)
    print(json.dumps(results))


@get.command()
def jobs():
    """
    Fetch all jobs
    """
    try:
        results = api.get_jobs()
    except DkronException as ex:
        print("Error while fetching: %s" % str(ex))
        exit(1)
    print(json.dumps(results))


@get.command(name="job")
def get_job(job_name: str = JobName):
    """
    Fetch specific job
    """
    try:
        results = api.get_job(job_name)
    except DkronException as ex:
        print("Error while fetching: %s" % str(ex))
        exit(1)
    print(json.dumps(results))


@get.command()
def executions(job_name: str = JobName):
    """
    Get system executions
    """
    try:
        results = api.get_executions(job_name)
    except DkronException as ex:
        print("Error while fetching: %s" % str(ex))
        exit(1)
    print(json.dumps(results))


@apply.command(name="job")
def apply_job(
    json_file_path: List[str] = typer.Argument(..., help="Path to the json file")
):
    """
    Create or update job(s)
    """
    for file_path in json_file_path:
        with open(file_path, "r") as json_file:
            data = json.load(json_file)
            try:
                api.apply_job(data)
            except DkronException as ex:
                print("Error while applying %s: %s" % (file_path, str(ex)))
                exit(1)
            print("Processed: %s" % file_path)


@app.command()
def run(job_name: str = JobName):
    """
    Execute job on demand
    """
    try:
        api.run_job(job_name)
    except DkronException as ex:
        print("Error while executing: %s" % str(ex))
        exit(1)


@app.command(name="export")
def export(backup_dir: str = typer.Argument(..., help="Path to the backup directory")):
    """
    Exports all jobs to json files
    """
    try:
        jobs = api.get_jobs()
        for job in jobs:
            filename = os.path.join(backup_dir, job["name"] + ".json")
            json.dump(job, open(filename, mode="w"), indent=2)
    except DkronException as ex:
        print("Error while fetching: %s" % str(ex))
        exit(1)


@delete.command(name="job")
def delete_job(job_name):
    """
    Delete job
    """
    try:
        api.delete_job(job_name)
    except DkronException as ex:
        print("Error while deleteing: %s" % str(ex))
        exit(1)


if __name__ == "__main__":
    app()
