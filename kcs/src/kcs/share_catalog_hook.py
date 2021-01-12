from io import StringIO
from typing import Optional, Dict, Any

import yaml
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.versioning import Journal


def _read_github_repo_file(access_token, repo_name, filepath, branch='master'):
    from base64 import b64decode
    from github import Github

    g = Github(access_token)
    repo = g.get_repo(repo_name)
    file_data = repo.get_contents(filepath, ref=branch).content
    github_file = b64decode(file_data).decode("utf8")
    return github_file


def _read_github_gist_file(access_token, gist_id, gist_filename):
    from github import Github

    g = Github(access_token)
    github_file = g.get_gist(gist_id).files[gist_filename].content
    return github_file


class GithubCatalogHook:

    @hook_impl
    def register_catalog(
        self,
        catalog: Optional[Dict[str, Dict[str, Any]]],
        credentials: Dict[str, Dict[str, Any]],
        load_versions: Dict[str, str],
        save_version: str,
        journal: Journal,
    ) -> DataCatalog:

        github_access_token = credentials['github_access_token']

        # file_content = _read_github_repo_file(
        #     github_access_token,
        #     'tamsanh/kedro-introduction-tutorial',
        #     '/conf/base/catalog.yml'
        # )

        file_content = _read_github_gist_file(
            github_access_token,
            '59c1513b14399c7d640dbe61823e9073',
            'example_shared_catalog.yml'
        )

        sio = StringIO(file_content)
        loaded_config = yaml.load(sio)

        return DataCatalog.from_config(
            {**loaded_config, **catalog}, credentials, load_versions, save_version, journal
        )