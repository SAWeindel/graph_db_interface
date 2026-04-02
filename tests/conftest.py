import os
import sys
import pytest
from graph_db_interface import GraphDB, GraphDBCredentials

GLOBAL_NAMED_GRAPH = "http://example.org/global_named_graph"
LOCAL_NAMED_GRAPH = "http://example.org/local_named_graph"
SHACL_SHAPE_GRAPH = "http://rdf4j.org/schema/rdf4j#SHACLShapeGraph"

TEST_GRAPHS = [None, GLOBAL_NAMED_GRAPH, LOCAL_NAMED_GRAPH, SHACL_SHAPE_GRAPH]


@pytest.fixture(scope="session")
def db() -> GraphDB:
    """Fixture to create a GraphDB client."""
    for env_var in [
        "GRAPHDB_URL",
        "GRAPHDB_USERNAME",
        "GRAPHDB_PASSWORD",
        "GRAPHDB_TEST_REPOSITORY",
    ]:
        if os.getenv(env_var) is None:
            print(f"Missing environment variable '{env_var}'.", file=sys.stderr)
            sys.exit(1)

    credentials = GraphDBCredentials(
        base_url=os.getenv("GRAPHDB_URL"),
        username=os.getenv("GRAPHDB_USERNAME"),
        password=os.getenv("GRAPHDB_PASSWORD"),
        repository=os.getenv("GRAPHDB_TEST_REPOSITORY"),
    )

    db = GraphDB(credentials=credentials)

    for graph in TEST_GRAPHS:
        db.clear_graph(graph)

    return db
