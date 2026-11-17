import pytest

from globusiri.config import Config, PathMap, parse_duration
from globusiri.errors import ConfigError


def test_parse_duration():
    assert parse_duration("1800") == 1800
    assert parse_duration(90) == 90
    assert parse_duration("30m") == 1800
    assert parse_duration("6h") == 21600
    assert parse_duration("2d") == 172800
    assert parse_duration(None, 5) == 5
    with pytest.raises(ConfigError):
        parse_duration("soon")


def test_pathmap_identity_when_empty():
    pm = PathMap({})
    assert pm.to_globus("/global/cfs/cdirs/m2616/x") == "/global/cfs/cdirs/m2616/x"
    assert not pm


def test_pathmap_alcf():
    pm = PathMap({"/lus/eagle/projects": "/"})
    assert pm.to_globus("/lus/eagle/projects/ATLAS_workflow_ALCF/usatlas/IRI_workdir/jobs") == \
        "/ATLAS_workflow_ALCF/usatlas/IRI_workdir/jobs"
    assert pm.to_globus("/lus/eagle/projects") == "/"
    assert pm.to_hpc("/ATLAS_workflow_ALCF/x") == "/lus/eagle/projects/ATLAS_workflow_ALCF/x"
    # no partial-component match
    assert pm.to_globus("/lus/eagle/projectsX/a") == "/lus/eagle/projectsX/a"


def test_pathmap_longest_prefix_wins():
    pm = PathMap({"/a": "/x", "/a/b": "/y"})
    assert pm.to_globus("/a/b/c") == "/y/c"
    assert pm.to_globus("/a/c") == "/x/c"


def test_example_config_loads(cfg):
    assert set(cfg.sites) == {"NERSC_Perlmutter", "ALCF_CRUX"}
    crux = cfg.site("ALCF_CRUX")
    assert crux.facility == "alcf"
    assert crux.upload_method == "globus_https"
    assert crux.unsupported_resource_fields == {"process_count", "processes_per_node", "cpu_cores_per_process"}
    assert crux.https_path_map.to_globus(crux.work_dir) == "/ATLAS_workflow_ALCF/usatlas/IRI_workdir/jobs"
    assert crux.e2e_endpoint().id == "05d2c76a-e867-4f67-aa57-76edeb0beda0"
    nersc = cfg.site("NERSC_Perlmutter")
    assert nersc.work_dir_resource_id == nersc.resource_id
    assert nersc.https_collection_id is None


def test_token_paths_by_convention(cfg, work):
    from globusiri.tokens import manager

    assert manager("iri", cfg, cfg.site("ALCF_CRUX")).access_file == work.root / "tokens/iri/alcf.access.yaml"
    assert manager("https", cfg, cfg.site("ALCF_CRUX")).access_file == work.root / "tokens/https/ALCF_CRUX.access.yaml"
    assert manager("e2e", cfg).refresh_file == work.root / "tokens/e2e/refresh.yaml"


def test_token_file_override(work):
    raw = {"token_files": {"e2e.access": "elsewhere/transfer.yaml"}, "sites": {}}
    cfg = Config(raw, paths=work)
    from globusiri.tokens import manager

    assert manager("e2e", cfg).access_file == work.root / "elsewhere/transfer.yaml"


def test_unknown_endpoint_is_config_error(work):
    raw = {"sites": {"S": {"facility": "nersc", "iri": {"base_url": "u", "resource_id": "r"},
                           "remote": {"work_dir": "/w"}, "e2e": {"endpoint": "nope"}}}}
    with pytest.raises(ConfigError):
        Config(raw, paths=work)


def test_unknown_site(cfg):
    with pytest.raises(ConfigError):
        cfg.site("NOPE")


def test_upload_method_defaults_to_globus_https(work):
    import copy

    import yaml

    raw = yaml.safe_load(work.default_config.read_text())
    for site in raw["sites"].values():
        site["remote"].pop("upload_method", None)
    cfg = Config(copy.deepcopy(raw), paths=work)
    assert {s.upload_method for s in cfg.sites.values()} == {"globus_https"}
    del raw["sites"]["NERSC_Perlmutter"]["https"]
    with pytest.raises(ConfigError, match="upload_method is globus_https"):
        Config(raw, paths=work)


def test_iri_sfapi_retry_settings(work):
    import yaml

    from globusiri import services

    raw = yaml.safe_load(work.default_config.read_text())
    raw["sites"]["NERSC_Perlmutter"]["iri"].update(sfapi_retries=5, sfapi_retry_sleep="1m")
    cfg = Config(raw, paths=work)
    site = cfg.site("NERSC_Perlmutter")
    assert (site.iri_sfapi_retries, site.iri_sfapi_retry_sleep) == (5, 60)
    client = services.iri_client(cfg, site)
    assert (client.sfapi_retries, client.sfapi_retry_sleep) == (5, 60)
    crux = cfg.site("ALCF_CRUX")   # not set: the defaults
    assert (crux.iri_sfapi_retries, crux.iri_sfapi_retry_sleep) == (3, 3)
    raw["sites"]["NERSC_Perlmutter"]["iri"]["sfapi_retries"] = 0
    with pytest.raises(ConfigError, match="sfapi_retries must be >= 1"):
        Config(raw, paths=work)


def test_iri_and_https_retry_settings(work):
    import yaml

    from globusiri import services

    raw = yaml.safe_load(work.default_config.read_text())
    site_raw = raw["sites"]["NERSC_Perlmutter"]
    site_raw["iri"].update(retries=4, retry_sleep="30s")
    site_raw["https"].update(retries=5, retry_sleep="1m")
    cfg = Config(raw, paths=work)
    site = cfg.site("NERSC_Perlmutter")
    iri = services.iri_client(cfg, site)
    https = services.https_client(cfg, site)
    assert (iri.retry_attempts, iri.retry_sleep) == (4, 30)
    assert (https.retry_attempts, https.retry_sleep) == (5, 60)
    site_raw["https"]["retries"] = 0
    with pytest.raises(ConfigError, match="https.retries must be >= 1"):
        Config(raw, paths=work)
