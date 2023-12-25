import logging
import pickle
import os
from loop_software.loop_selector import WebSelector
from pathlib import Path
from datetime import datetime


class WebHost:
    """Class serves as config class for a particular website."""
    def __init__(self,
                 host_id: int,
                 name: str,
                 top_level_domain: str):
        self.host_id = host_id
        self.name = name
        self.top_level_domain = top_level_domain
        self.path: Path | None = None
        self.created_at = datetime.today()
        self.crawl_history: list[datetime] = list()
        self.recrawl: int | None = None
        self.content_selectors: list[WebSelector] | None = None
        self.table_name = self.name+str(self.host_id)
        self.request_settings: dict | None = None

    def save(self, path: Path = None) -> None:
        """Saves a host to self.path if path parameter is not fulfilled."""
        if self.path is None:
            assert path is not None
            self.path = Path(path)
        with self.path.joinpath(f"{self.name}.pkl").open('wb') as writer:
            serialized = pickle.dumps(self)
            writer.write(serialized)

    def format_crawl(self, content_selectors: list[WebSelector]) -> None:
        assert len(content_selectors) > 0
        self.content_selectors = content_selectors


def open_or_create_host(host_id: int,
                        name: str,
                        top_level_domain: str,
                        path: Path | None = None,
                        transform: bool = False) -> WebHost:
    """Creates a webhost if none exists, otherwise opens the host config.
        host_id: is an arbitrary number, there placed for purposes of having different configs for a single webhost,
        name: google, for instance, or ebay;
        top_level_domain: .com or .org or .de, etc.;
        path: Opens from pathlike object. If None opens from expected location in hosts folder,
        transform is not relevant, at the moment. Please don't change."""

    assert host_id is not None
    assert name is not None
    if not transform:
        host_path = Path(os.path.abspath(os.curdir)).joinpath("hosts", name + str(host_id))
    else:
        host_path = Path(os.path.abspath(os.curdir)).parent.joinpath("hosts", name + str(host_id))
    if path is None:
        path = Path(host_path)
    try:
        logging.basicConfig(level=logging.INFO,
                            format="[%(asctime)s] [%(levelname)s]: %(message)s",
                            handlers=[logging.FileHandler(path.joinpath(f"{name}.log"))])
    except (FileNotFoundError, PermissionError):
        os.makedirs(path)
        logging.basicConfig(level=logging.INFO,
                            format="[%(asctime)s] [%(levelname)s]: %(message)s",
                            handlers=[logging.FileHandler(path.joinpath(f"{name}.log"))])
    if Path.is_file(path.joinpath(f"{name}.pkl")):
        with open(path.joinpath(f"{name}.pkl"), 'rb') as pickled_host:
            host = pickle.load(pickled_host)
            host.host_path = path
        return host
    else:
        host = WebHost(host_id=host_id,
                       name=name,
                       top_level_domain=top_level_domain)
        host.path = path
        return host
