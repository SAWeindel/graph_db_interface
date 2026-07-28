"""Query-construction tests for `triples_update`.

These do not need a live GraphDB: they stub `query` and assert on the SPARQL text.
The blank-node behaviour they pin cannot be observed through the return value, and the
existing update tests in `test_graph_manipulation.py` all require a repository.
"""

import logging
import re

import pytest
from rdflib import BNode, Literal

from graph_db_interface import GraphDB, IRI


SUB = IRI("https://example.org/Belt1")
PRED = IRI("https://example.org/hasSpeed")
VALUE = IRI("https://example.org/hasValue")


@pytest.fixture
def stub_db() -> GraphDB:
    """A GraphDB that records the last query instead of sending it.

    Built with `__new__` because `__init__` authenticates against a live server.
    """
    db = GraphDB.__new__(GraphDB)
    db.logger = logging.getLogger("stub")
    db.named_graph = None
    db._repository = "stub"
    db._blank_ids = set()
    db.queries = []
    db.query = lambda query, update=False, **kwargs: (db.queries.append(query), True)[1]
    db.all_triple_exists = lambda triples, named_graph=None: True
    return db


def _variables(block: str) -> list[str]:
    return re.findall(r"\?\w+", block)


def _split_blocks(query: str) -> dict[str, str]:
    match = re.search(
        r"DELETE \{(?P<delete>.*?)\}\s*INSERT \{(?P<insert>.*?)\}\s*WHERE \{(?P<where>.*?)\}\s*$",
        query,
        re.DOTALL,
    )
    assert match is not None, f"unexpected query shape:\n{query}"
    return match.groupdict()


def test_blank_node_on_both_sides_is_one_variable(stub_db: GraphDB):
    """A retained blank node must keep its identity across the update.

    Regression: the DELETE and INSERT patterns used to be rendered from two separate
    blank-node maps, so the same node got two variables and the new one was minted by
    BIND(BNODE()) — replacing the node and orphaning anything not listed in old_triples.
    """
    node = BNode("genid-retained")

    stub_db.triples_update(
        old_triples=[(SUB, PRED, node), (node, VALUE, Literal(12.1))],
        new_triples=[(SUB, PRED, node), (node, VALUE, Literal(1.4))],
    )
    blocks = _split_blocks(stub_db.queries[-1])

    delete_vars = set(_variables(blocks["delete"]))
    insert_vars = set(_variables(blocks["insert"]))
    assert len(delete_vars) == 1
    assert delete_vars == insert_vars, "the retained node must be the same variable"
    assert "BIND(BNODE()" not in blocks["where"], "a retained node must not be minted"


def test_blank_node_only_in_new_triples_is_minted(stub_db: GraphDB):
    """A genuinely new anonymous node still gets a fresh store node."""
    fresh = BNode("genid-fresh")

    stub_db.triples_update(
        old_triples=[(SUB, PRED, Literal("gone"))],
        new_triples=[(SUB, PRED, fresh), (fresh, VALUE, Literal(1.4))],
    )
    blocks = _split_blocks(stub_db.queries[-1])

    insert_vars = set(_variables(blocks["insert"]))
    assert len(insert_vars) == 1
    assert blocks["where"].count("BIND(BNODE()") == 1
    assert insert_vars.pop() in blocks["where"]


def test_mixed_retained_and_new_blank_nodes(stub_db: GraphDB):
    """Only the new-side node is minted; the retained one stays bound by the WHERE."""
    retained, fresh = BNode("genid-retained"), BNode("genid-fresh")

    stub_db.triples_update(
        old_triples=[(SUB, PRED, retained), (retained, VALUE, Literal(12.1))],
        new_triples=[
            (SUB, PRED, retained),
            (retained, VALUE, Literal(1.4)),
            (SUB, PRED, fresh),
            (fresh, VALUE, Literal(9.9)),
        ],
    )
    blocks = _split_blocks(stub_db.queries[-1])

    assert blocks["where"].count("BIND(BNODE()") == 1
    retained_var = set(_variables(blocks["delete"]))
    assert len(retained_var) == 1
    assert retained_var <= set(_variables(blocks["insert"]))
    assert len(set(_variables(blocks["insert"]))) == 2


def test_no_blank_nodes_is_unchanged(stub_db: GraphDB):
    """The ordinary IRI-only path emits no variables and no BIND."""
    stub_db.triples_update(
        old_triples=[(SUB, VALUE, Literal(12.1))],
        new_triples=[(SUB, VALUE, Literal(1.4))],
    )
    query = stub_db.queries[-1]

    assert "?" not in query
    assert "BIND(BNODE()" not in query
