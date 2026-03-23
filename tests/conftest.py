import pytest
from infra.storage import ConfigLoader


@pytest.fixture(autouse=True)
def reset_config_loader():
    """每个测试前后重置 ConfigLoader 单例，防止测试间状态污染。"""
    ConfigLoader.reset()
    yield
    ConfigLoader.reset()
