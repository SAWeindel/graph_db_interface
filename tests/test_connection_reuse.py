import threading

import urllib3.connectionpool
import pytest

from graph_db_interface import GraphDB


@pytest.fixture
def count_new_connections(monkeypatch):
    """Count TCP connections opened by urllib3 while the fixture is active.

    A client that reuses one session keeps urllib3's connection pool alive between
    calls, so repeated queries open no new connection. A client that goes through the
    module-level `requests` helpers builds a fresh session — and therefore a fresh
    pool — per call, so every query opens one.
    """
    opened = []

    for pool_cls in (
        urllib3.connectionpool.HTTPConnectionPool,
        urllib3.connectionpool.HTTPSConnectionPool,
    ):
        original = pool_cls._new_conn

        def counting_new_conn(self, _original=original):
            opened.append(type(self).__name__)
            return _original(self)

        monkeypatch.setattr(pool_cls, "_new_conn", counting_new_conn)

    return opened


def test_repeated_queries_reuse_one_connection(db: GraphDB, count_new_connections):
    """Repeated queries must not re-handshake.

    Against a remote HTTPS endpoint the TCP connect plus TLS handshake costs more than
    the query itself, so a per-call connection makes every query several times slower.
    """
    for _ in range(3):
        db.query("ASK { ?s ?p ?o }")

    assert count_new_connections == [], (
        f"expected the warm connection pool to be reused, but "
        f"{len(count_new_connections)} new connection(s) were opened: {count_new_connections}"
    )


def test_client_holds_a_persistent_session(db: GraphDB):
    """The session is one object for the thread's lifetime, not one per request."""
    before = db._session
    db.query("ASK { ?s ?p ?o }")

    assert db._session is before


def test_each_thread_gets_its_own_session(db: GraphDB):
    """One client is driven from several threads at once — a web framework serving requests
    while the embedding code queries. A `requests.Session` mutates its cookie jar on every
    response and is not thread-safe, so threads must not share one.
    """
    sessions = []

    def query_from_thread():
        db.query("ASK { ?s ?p ?o }")
        sessions.append(db._session)

    threads = [threading.Thread(target=query_from_thread) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert len(sessions) == 3, "a worker thread failed to complete its query"
    assert len(set(map(id, sessions))) == 3, "threads shared a session"
    assert db._session not in sessions, "a worker thread reused the main thread's session"
