# To be imported into ..graph_db.py GraphDB class

from typing import Optional, TYPE_CHECKING
from graph_db_interface.utils import utils
from graph_db_interface.utils.iri import IRI
from graph_db_interface.utils.types import (
    PartialTripleLike,
    SubjectLike,
    PredicateLike,
    ObjectLike,
    GraphNameLike,
    TripleLike,
)
from graph_db_interface.exceptions import InvalidInputError, GraphDbException

from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def triple_exists(
    self: "GraphDB",
    triple: TripleLike,
    named_graph: Optional[GraphNameLike] = None,
) -> bool:
    """
    Check whether a specific triple exists in the graph database.

    Args:
        triple (TripleLike): The triple `(subject, predicate, object)` to check.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.

    Returns:
        bool: True if the triple exists, False otherwise.
    """
    triple = utils.sanitize_triple(triple)
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    query = SPARQLQuery.ask(
        where_clauses=[
            utils.triple_to_string(triple, "."),
        ],
        named_graph=named_graph,
    )
    result = self.query(query=query)
    if result is None or result["boolean"] is False:
        self.logger.debug(
            f"Unable to find triple ({utils.triple_to_string(triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Found triple ({utils.triple_to_string(triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )
    return True


def triple_add(
    self: "GraphDB",
    triple: TripleLike,
    named_graph: Optional[GraphNameLike] = None,
) -> None:
    """
    Add a triple to the graph database.

    Args:
        triple (TripleLike): The triple `(subject, predicate, object)` to insert.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.
    """
    triple = utils.sanitize_triple(triple)
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    query = SPARQLQuery.insert_data(
        triples=[triple],
        named_graph=named_graph,
    )
    self.query(query=query, update=True)

    self.logger.debug(
        f"Successfully inserted triple: ({utils.triple_to_string(triple)}) named_graph: {named_graph or "default"}, repository: {self._repository}"
    )


def triple_delete(
    self: "GraphDB",
    triple: TripleLike,
    check_exist: Optional[bool] = True,
    named_graph: Optional[GraphNameLike] = None,
) -> None:
    """
    Delete a single triple.

    A SPARQL DELETE operation in GraphDB can be successful even if the triple
    does not exist. When `check_exist=True`, the function verifies the triple is
    present before attempting deletion and raises an error if not found.

    Args:
        triple (TripleLike): The triple `(subject, predicate, object)` to delete.
        check_exist (Optional[bool]): Whether to verify existence prior to deletion. Defaults to True.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.
    """
    triple = utils.sanitize_triple(triple)
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    if check_exist:
        if not self.triple_exists(triple, named_graph=named_graph):
            error_msg = "Unable to delete triple since it does not exist"
            self.logger.warning(error_msg)
            raise GraphDbException(error_msg)

    query = SPARQLQuery.delete_data(
        triples=[triple],
        named_graph=named_graph,
    )
    self.query(query=query, update=True)

    self.logger.debug(
        f"Successfully deleted triple: ({utils.triple_to_string(triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )


def triple_update(
    self: "GraphDB",
    old_triple: TripleLike,
    new_triple: Optional[PartialTripleLike] = None,
    new_sub: Optional[SubjectLike] = None,
    new_pred: Optional[PredicateLike] = None,
    new_obj: Optional[ObjectLike] = None,
    check_exist: Optional[bool] = True,
    named_graph: Optional[GraphNameLike] = None,
) -> bool:
    """
    Update a triple by replacing any of its parts.

    Performs a SPARQL `DELETE ... INSERT ... WHERE` that replaces the old triple
    with a new triple built from provided parts.

    Args:
        old_triple (TripleLike): Existing triple to update.
        new_triple (Optional[PartialTripleLike]): Replacement values (subject/predicate/object). Use this
            or `new_sub`/`new_pred`/`new_obj`.
        new_sub (Optional[SubjectLike]): Replacement subject.
        new_pred (Optional[PredicateLike]): Replacement predicate.
        new_obj (Optional[ObjectLike]): Replacement object.
        check_exist (Optional[bool]): If True, verify that `old_triple` exists before updating. Defaults to True.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.
    """
    elems_given = new_sub is not None or new_pred is not None or new_obj is not None
    if (new_triple and elems_given) or (not new_triple and not elems_given):
        raise InvalidInputError(
            "Either 'new triple' or 'new_sub/new_pred/new_obj' must be provided, not both."
        )

    old_triple = utils.sanitize_triple(old_triple)
    new_triple = utils.sanitize_triple(
        new_triple or (new_sub, new_pred, new_obj),
        allow_partial=True,
    )

    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    if check_exist:
        if not self.triple_exists(old_triple, named_graph=named_graph):
            error_msg = f"Triple does not exist: ({utils.triple_to_string(old_triple)})"
            self.logger.warning(error_msg)
            raise GraphDbException(error_msg)

    # Determine replacement variables
    update_triple = tuple(n if n else o for o, n in zip(old_triple, new_triple))

    query = SPARQLQuery.delete_insert_data(
        delete_triples=[old_triple],
        insert_triples=[update_triple],
        where_clauses=[utils.triple_to_string(old_triple, ".")],
        named_graph=named_graph,
    )
    self.query(query=query, update=True)

    self.logger.debug(
        f"Successfully updated triple to: ({utils.triple_to_string(update_triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )
