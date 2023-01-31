from lib.utils import Settings


def test_nothing() -> None:
    """trivial test"""
    s = Settings()
    assert s.cache
    assert s.use_cache
