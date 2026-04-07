"""
Test suite for SHACL validation functionality in GraphDB.

This module tests the SHACL (Shapes Constraint Language) validation capabilities,
including:
- Uploading SHACL shapes to the repository
- Automatic validation during data updates (add/delete/update operations)
- Handling validation reports for both valid and invalid data
- Proper exception raising for constraint violations
- Atomic rollback when validation fails

SHACL validation is automatically performed during all update operations
(using db.query with update=True). When validation fails, a SHACLValidationError
is raised with the complete validation report, and all changes are rolled back.
"""

import pytest
from rdflib import Literal, XSD
from graph_db_interface import GraphDB
from graph_db_interface.exceptions import SHACLValidationError
from graph_db_interface.utils.iri import IRI

from .conftest import (
    GLOBAL_NAMED_GRAPH,
    LOCAL_NAMED_GRAPH,
    SHACL_SHAPE_GRAPH,
    TEST_GRAPHS,
)

# Test vocabulary namespace
TEST_NS = "http://example.org/test#"

# Valid test data - Module with exactly one possession (conforms to SHACL)
VALID_MODULE = f"{TEST_NS}validModule"
VALID_POSSESSION = f"{TEST_NS}possession1"

# Invalid test data - Module with multiple possessions (violates maxCount)
INVALID_MODULE_MULTI = f"{TEST_NS}invalidModuleMulti"
INVALID_POSSESSION_1 = f"{TEST_NS}possession2"
INVALID_POSSESSION_2 = f"{TEST_NS}possession3"

# Invalid test data - Module with no possessions (violates minCount)
INVALID_MODULE_NONE = f"{TEST_NS}invalidModuleNone"

# Test predicates
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
HAS_POSSESSION = f"{TEST_NS}hasPossession"
HAS_NAME = f"{TEST_NS}hasName"

# Test classes
MODULE_CLASS = f"{TEST_NS}Module"
CARRIER_CLASS = f"{TEST_NS}Carrier"


# SHACL Shape definition as a string in Turtle format
SHACL_SHAPE = f"""
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix test: <{TEST_NS}> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

test:ModuleShape
    a sh:NodeShape ;
    sh:targetClass test:Module ;
    sh:property [
        sh:path test:hasPossession ;
        sh:minCount 1 ;
        sh:maxCount 1 ;
        sh:class test:Carrier ;
        sh:name "Module possession constraint" ;
        sh:description "A module must have exactly one carrier possession" ;
        sh:severity sh:Violation
    ] ;
    sh:property [
        sh:path test:hasName ;
        sh:minCount 1 ;
        sh:maxCount 1 ;
        sh:datatype xsd:string ;
        sh:name "Module name constraint" ;
        sh:description "A module must have exactly one name (string)" ;
        sh:severity sh:Violation
    ] .
"""


@pytest.fixture(params=[None, LOCAL_NAMED_GRAPH], scope="module")
def named_graph(request) -> str:
    # Provide a per-test local override for the named graph
    return request.param


@pytest.fixture(params=[None, GLOBAL_NAMED_GRAPH], scope="function", autouse=True)
def setup(request, db: GraphDB):
    # Set or unset the global named graph on the DB client
    db.named_graph = request.param

    # clear all test graphs before each test
    for graph in TEST_GRAPHS:
        db.clear_graph(graph)


@pytest.fixture(scope="function")
def setup_shacl_shapes(db: GraphDB):
    """
    Upload SHACL shapes to the repository before each test.

    This fixture ensures that SHACL shapes are properly loaded into
    the designated shapes graph for validation.
    """
    # Upload SHACL shapes to a dedicated named graph using import_statements
    result = db.import_statements(
        content=SHACL_SHAPE,
        overwrite=True,
        graph_iri=SHACL_SHAPE_GRAPH,
        content_type="application/x-turtle",
    )
    assert result is True


def test_upload_shacl_shapes(db: GraphDB):
    """
    Test uploading SHACL shapes to the repository using import_statements.
    We however cannot directly poll for their existence.
    """
    result = db.import_statements(
        content=SHACL_SHAPE,
        overwrite=True,
        graph_iri=SHACL_SHAPE_GRAPH,
        content_type="application/x-turtle",
    )
    assert result is True


def test_validate_valid_data(db: GraphDB, named_graph: str, setup_shacl_shapes):
    """
    Test SHACL validation with valid data that conforms to constraints.

    Expected behavior:
    - Data insertion succeeds without raising exceptions
    - All triples are added successfully
    """
    # Add a valid module with exactly one possession and one name
    # This should succeed because it conforms to SHACL constraints
    db.triples_add(
        [
            (VALID_MODULE, RDF_TYPE, MODULE_CLASS),
            (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
            (VALID_MODULE, HAS_NAME, Literal("Valid Module", datatype=XSD.string)),
            (VALID_POSSESSION, RDF_TYPE, CARRIER_CLASS),
        ],
        named_graph=named_graph,
    )

    # Verify the data was actually added
    assert db.triple_exists(
        (VALID_MODULE, RDF_TYPE, MODULE_CLASS),
        named_graph=named_graph,
    )
    assert db.triple_exists(
        (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
        named_graph=named_graph,
    )


def test_validate_invalid_data_maxcount(
    db: GraphDB, named_graph: str, setup_shacl_shapes
):
    """
    Test SHACL validation with invalid data that violates maxCount constraint.

    Expected behavior:
    - Data insertion fails and raises SHACLValidationError
    - Validation report contains constraint violation details
    - No data is added to the repository (rollback)
    """
    # Attempt to add an invalid module with two possessions (violates maxCount = 1)
    with pytest.raises(SHACLValidationError) as exc_info:
        db.triples_add(
            [
                (INVALID_MODULE_MULTI, RDF_TYPE, MODULE_CLASS),
                (INVALID_MODULE_MULTI, HAS_POSSESSION, INVALID_POSSESSION_1),
                (INVALID_MODULE_MULTI, HAS_POSSESSION, INVALID_POSSESSION_2),
                (
                    INVALID_MODULE_MULTI,
                    HAS_NAME,
                    Literal("Invalid Module", datatype=XSD.string),
                ),
                (INVALID_POSSESSION_1, RDF_TYPE, CARRIER_CLASS),
                (INVALID_POSSESSION_2, RDF_TYPE, CARRIER_CLASS),
            ],
            named_graph=named_graph,
        )

    error_report = exc_info.value.message
    report_iri, result_iri, shape_iri = split_report_lines(error_report)

    report_lines = {
        f'{report_iri} <http://www.w3.org/ns/shacl#conforms> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .',
        f"{report_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#ValidationReport> .",
        f'{report_iri} <http://rdf4j.org/schema/rdf4j#truncated> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .',
        f"{report_iri} <http://www.w3.org/ns/shacl#result> {result_iri} .",
        f"{result_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#ValidationResult> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#focusNode> <http://example.org/test#invalidModuleMulti> .",
        f"{result_iri} <http://rdf4j.org/shacl-extensions#shapesGraph> <http://rdf4j.org/schema/rdf4j#SHACLShapeGraph> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#resultPath> <http://example.org/test#hasPossession> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#sourceConstraintComponent> <http://www.w3.org/ns/shacl#MaxCountConstraintComponent> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#resultSeverity> <http://www.w3.org/ns/shacl#Violation> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#sourceShape> {shape_iri} .",
        f"{shape_iri} <http://www.w3.org/ns/shacl#severity> <http://www.w3.org/ns/shacl#Violation> .",
        f"{shape_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#PropertyShape> .",
        f"{shape_iri} <http://www.w3.org/ns/shacl#path> <http://example.org/test#hasPossession> .",
        f'{shape_iri} <http://www.w3.org/ns/shacl#maxCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .',
    }

    for line in report_lines:
        assert line in error_report

    # Verify no data was added (rollback successful)
    assert not db.iri_exists(
        iri=INVALID_MODULE_MULTI, as_sub=True, named_graph=named_graph
    )


def test_validate_invalid_data_mincount(
    db: GraphDB, named_graph: str, setup_shacl_shapes
):
    """
    Test SHACL validation with invalid data that violates minCount constraint.

    Expected behavior:
    - Data insertion fails and raises SHACLValidationError
    - Validation report contains constraint violation details
    - No data is added to the repository (rollback)
    """
    # Attempt to add an invalid module with no possessions (violates minCount = 1)
    with pytest.raises(SHACLValidationError) as exc_info:
        db.triples_add(
            [
                (INVALID_MODULE_NONE, RDF_TYPE, MODULE_CLASS),
                (
                    INVALID_MODULE_NONE,
                    HAS_NAME,
                    Literal("Module No Possession", datatype=XSD.string),
                ),
            ],
            named_graph=named_graph,
        )

    error_report = exc_info.value.message
    report_iri, result_iri, shape_iri = split_report_lines(error_report)

    report_lines = {
        f'{report_iri} <http://www.w3.org/ns/shacl#conforms> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .',
        f"{report_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#ValidationReport> .",
        f'{report_iri} <http://rdf4j.org/schema/rdf4j#truncated> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .',
        f"{report_iri} <http://www.w3.org/ns/shacl#result> {result_iri} .",
        f"{result_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#ValidationResult> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#focusNode> <http://example.org/test#invalidModuleNone> .",
        f"{result_iri} <http://rdf4j.org/shacl-extensions#shapesGraph> <http://rdf4j.org/schema/rdf4j#SHACLShapeGraph> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#resultPath> <http://example.org/test#hasPossession> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#sourceConstraintComponent> <http://www.w3.org/ns/shacl#MinCountConstraintComponent> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#resultSeverity> <http://www.w3.org/ns/shacl#Violation> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#sourceShape> {shape_iri} .",
        f"{shape_iri} <http://www.w3.org/ns/shacl#severity> <http://www.w3.org/ns/shacl#Violation> .",
        f"{shape_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#PropertyShape> .",
        f"{shape_iri} <http://www.w3.org/ns/shacl#path> <http://example.org/test#hasPossession> .",
        f'{shape_iri} <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .',
    }

    for line in report_lines:
        assert line in error_report

    # Verify no data was added (rollback successful)
    assert not db.iri_exists(
        iri=INVALID_MODULE_NONE, as_sub=True, named_graph=named_graph
    )


def test_update_valid(db: GraphDB, named_graph: str, setup_shacl_shapes):
    """
    Test update operation with valid data.

    Expected behavior:
    - Update succeeds when data conforms to SHACL constraints
    - Data is actually updated in the repository
    - No exceptions are raised
    """
    # First add valid initial data
    db.triples_add(
        [
            (VALID_MODULE, RDF_TYPE, MODULE_CLASS),
            (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
            (VALID_MODULE, HAS_NAME, Literal("Original Name", datatype=XSD.string)),
            (VALID_POSSESSION, RDF_TYPE, CARRIER_CLASS),
        ],
        named_graph=named_graph,
    )

    # Perform an update that maintains SHACL conformance
    db.triple_update(
        old_triple=(
            VALID_MODULE,
            HAS_NAME,
            Literal("Original Name", datatype=XSD.string),
        ),
        new_triple=(
            VALID_MODULE,
            HAS_NAME,
            Literal("Updated Name", datatype=XSD.string),
        ),
        named_graph=named_graph,
    )

    # Verify the update was applied
    updated_exists = db.triple_exists(
        (VALID_MODULE, HAS_NAME, Literal("Updated Name", datatype=XSD.string)),
        named_graph=named_graph,
    )
    assert updated_exists is True

    original_exists = db.triple_exists(
        (VALID_MODULE, HAS_NAME, Literal("Original Name", datatype=XSD.string)),
        named_graph=named_graph,
    )
    assert original_exists is False


def test_add_invalid_maxcount(db: GraphDB, named_graph: str, setup_shacl_shapes):
    """
    Test add operation with invalid data (maxCount violation).

    Expected behavior:
    - Add fails when resulting data would violate SHACL constraints
    - SHACLValidationError is raised with validation report
    - No changes are made to the repository (rollback)
    """
    # First add valid initial data
    db.triples_add(
        [
            (VALID_MODULE, RDF_TYPE, MODULE_CLASS),
            (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
            (VALID_MODULE, HAS_NAME, Literal("Valid Name", datatype=XSD.string)),
            (VALID_POSSESSION, RDF_TYPE, CARRIER_CLASS),
            (INVALID_POSSESSION_1, RDF_TYPE, CARRIER_CLASS),
        ],
        named_graph=named_graph,
    )

    # Attempt to add a second possession that would violate maxCount constraint
    with pytest.raises(SHACLValidationError) as exc_info:
        db.triple_add(
            triple=(VALID_MODULE, HAS_POSSESSION, INVALID_POSSESSION_1),
            named_graph=named_graph,
        )

    error_report = exc_info.value.message
    report_iri, result_iri, shape_iri = split_report_lines(error_report)

    report_lines = {
        f'{report_iri} <http://www.w3.org/ns/shacl#conforms> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .',
        f"{report_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#ValidationReport> .",
        f'{report_iri} <http://rdf4j.org/schema/rdf4j#truncated> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .',
        f"{report_iri} <http://www.w3.org/ns/shacl#result> {result_iri} .",
        f"{result_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#ValidationResult> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#focusNode> <http://example.org/test#validModule> .",
        f"{result_iri} <http://rdf4j.org/shacl-extensions#shapesGraph> <http://rdf4j.org/schema/rdf4j#SHACLShapeGraph> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#resultPath> <http://example.org/test#hasPossession> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#sourceConstraintComponent> <http://www.w3.org/ns/shacl#MaxCountConstraintComponent> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#resultSeverity> <http://www.w3.org/ns/shacl#Violation> .",
        f"{result_iri} <http://www.w3.org/ns/shacl#sourceShape> {shape_iri} .",
        f"{shape_iri} <http://www.w3.org/ns/shacl#severity> <http://www.w3.org/ns/shacl#Violation> .",
        f"{shape_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#PropertyShape> .",
        f"{shape_iri} <http://www.w3.org/ns/shacl#path> <http://example.org/test#hasPossession> .",
        f'{shape_iri} <http://www.w3.org/ns/shacl#maxCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .',
    }

    for line in report_lines:
        assert line in error_report

    # Verify no changes were made (rollback successful)
    invalid_triple_exists = db.triple_exists(
        (VALID_MODULE, HAS_POSSESSION, INVALID_POSSESSION_1),
        named_graph=named_graph,
    )
    assert invalid_triple_exists is False

    # Original valid data should still exist
    valid_triple_exists = db.triple_exists(
        (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
        named_graph=named_graph,
    )
    assert valid_triple_exists is True


def test_delete_invalid_mincount(db: GraphDB, named_graph: str, setup_shacl_shapes):
    """
    Test delete operation that would violate minCount constraint.

    Expected behavior:
    - Delete fails when resulting data would violate SHACL constraints
    - SHACLValidationError is raised with validation report
    - No changes are made to the repository (rollback)
    """
    # First add valid initial data
    db.triples_add(
        [
            (VALID_MODULE, RDF_TYPE, MODULE_CLASS),
            (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
            (VALID_MODULE, HAS_NAME, Literal("Valid Name", datatype=XSD.string)),
            (VALID_POSSESSION, RDF_TYPE, CARRIER_CLASS),
        ],
        named_graph=named_graph,
    )

    # Attempt to delete the only possession (would violate minCount = 1)
    with pytest.raises(SHACLValidationError) as exc_info:
        db.triple_delete(
            triple=(VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
            named_graph=named_graph,
        )

    # Verify the exception contains validation details in RDF format
    error_report = exc_info.value.message
    assert "MinCount" in error_report or "minCount" in error_report
    assert "conforms" in error_report
    assert '"false"' in error_report
    assert "MinCountConstraintComponent" in error_report

    # Verify no changes were made (rollback successful)
    triple_still_exists = db.triple_exists(
        (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
        named_graph=named_graph,
    )
    assert triple_still_exists is True


def test_batch_update_valid(db: GraphDB, named_graph: str, setup_shacl_shapes):
    """
    Test batch update operation with valid data.

    Expected behavior:
    - Batch update succeeds when all data conforms to SHACL constraints
    - All updates are applied atomically
    - No exceptions are raised
    """
    # First add valid initial data
    db.triples_add(
        [
            (VALID_MODULE, RDF_TYPE, MODULE_CLASS),
            (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
            (VALID_MODULE, HAS_NAME, Literal("Original Name", datatype=XSD.string)),
            (VALID_POSSESSION, RDF_TYPE, CARRIER_CLASS),
            (INVALID_POSSESSION_1, RDF_TYPE, CARRIER_CLASS),
        ],
        named_graph=named_graph,
    )

    # Perform batch updates that maintain SHACL conformance
    db.triples_update(
        old_triples=[
            (VALID_MODULE, HAS_NAME, Literal("Original Name", datatype=XSD.string)),
            (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
        ],
        new_triples=[
            (VALID_MODULE, HAS_NAME, Literal("New Name", datatype=XSD.string)),
            (VALID_MODULE, HAS_POSSESSION, INVALID_POSSESSION_1),
        ],
        named_graph=named_graph,
    )

    # Verify updates were applied
    assert db.triple_exists(
        (VALID_MODULE, HAS_NAME, Literal("New Name", datatype=XSD.string)),
        named_graph=named_graph,
    )
    assert db.triple_exists(
        (VALID_MODULE, HAS_POSSESSION, INVALID_POSSESSION_1),
        named_graph=named_graph,
    )


def test_batch_add_invalid(db: GraphDB, named_graph: str, setup_shacl_shapes):
    """
    Test batch add operation with invalid data.

    Expected behavior:
    - Batch add fails when resulting data would violate SHACL constraints
    - SHACLValidationError is raised
    - NO changes are made to the repository (atomic rollback)
    """
    # First add valid initial data
    db.triples_add(
        [
            (VALID_MODULE, RDF_TYPE, MODULE_CLASS),
            (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
            (VALID_MODULE, HAS_NAME, Literal("Original Name", datatype=XSD.string)),
            (VALID_POSSESSION, RDF_TYPE, CARRIER_CLASS),
            (INVALID_POSSESSION_1, RDF_TYPE, CARRIER_CLASS),
            (INVALID_POSSESSION_2, RDF_TYPE, CARRIER_CLASS),
        ],
        named_graph=named_graph,
    )

    # Attempt batch add that would add multiple possessions (violates maxCount)
    with pytest.raises(SHACLValidationError):
        db.triples_add(
            triples_to_add=[
                (VALID_MODULE, HAS_POSSESSION, INVALID_POSSESSION_1),
                (VALID_MODULE, HAS_POSSESSION, INVALID_POSSESSION_2),
            ],
            named_graph=named_graph,
        )

    # Verify NO changes were made (atomic rollback)
    assert (
        db.triple_exists(
            (VALID_MODULE, HAS_POSSESSION, VALID_POSSESSION),
            named_graph=named_graph,
        )
        is True
    )

    assert (
        db.triple_exists(
            (VALID_MODULE, HAS_POSSESSION, INVALID_POSSESSION_1),
            named_graph=named_graph,
        )
        is False
    )

    assert (
        db.triple_exists(
            (VALID_MODULE, HAS_POSSESSION, INVALID_POSSESSION_2),
            named_graph=named_graph,
        )
        is False
    )


def test_validation_report_structure(db: GraphDB, named_graph: str, setup_shacl_shapes):
    """
    Test that validation reports have the correct RDF structure matching the SHACL spec.

    Expected report structure (in RDF/N-Triples format):
    - sh:conforms "false" (as boolean literal)
    - sh:result with validation results
    - Each result contains:
        - sh:focusNode: the node that violated constraints
        - sh:resultPath: the property path
        - sh:sourceConstraintComponent: the type of constraint violated
        - sh:resultSeverity: severity level (sh:Violation)
        - sh:sourceShape: the shape that was violated
    """
    # Attempt to add invalid data that violates SHACL constraints
    with pytest.raises(SHACLValidationError) as exc_info:
        db.triples_add(
            [
                (INVALID_MODULE_MULTI, RDF_TYPE, MODULE_CLASS),
                (INVALID_MODULE_MULTI, HAS_POSSESSION, INVALID_POSSESSION_1),
                (INVALID_MODULE_MULTI, HAS_POSSESSION, INVALID_POSSESSION_2),
                (INVALID_MODULE_MULTI, HAS_NAME, Literal("Test", datatype=XSD.string)),
                (INVALID_POSSESSION_1, RDF_TYPE, CARRIER_CLASS),
                (INVALID_POSSESSION_2, RDF_TYPE, CARRIER_CLASS),
            ],
            named_graph=named_graph,
        )

    # Get validation report from the exception (RDF string)
    report_rdf = exc_info.value.message

    # Verify report contains SHACL validation report structure
    assert "ValidationReport" in report_rdf
    assert "conforms" in report_rdf
    assert '"false"' in report_rdf

    # Verify result triples are present
    assert "ValidationResult" in report_rdf
    assert "focusNode" in report_rdf
    assert "resultPath" in report_rdf
    assert "sourceConstraintComponent" in report_rdf
    assert "resultSeverity" in report_rdf

    # Verify specific values for this test case
    assert INVALID_MODULE_MULTI in report_rdf
    assert HAS_POSSESSION in report_rdf
    assert "MaxCountConstraintComponent" in report_rdf
    assert "Violation" in report_rdf


def split_report_lines(error_report: str) -> tuple[str, str, str]:
    """
    Helper function to split a validation report string into individual RDF triples.

    This function handles splitting the report into lines, removing newlines, and
    ensuring that each triple is properly separated for easier assertion checks.

    Args:
        report (str): The raw validation report string in RDF format.

    Returns:
        tuple[str, str, str]: A tuple containing the IRI of the conforms property, the IRI of the source shape, and the IRI of the report.
    """
    assert "<http://www.w3.org/ns/shacl#sourceShape>" in error_report
    assert "<http://www.w3.org/ns/shacl#result>" in error_report
    report_split = error_report.replace(".\n", "").split(" ")
    result_index = report_split.index("<http://www.w3.org/ns/shacl#result>")
    shape_index = report_split.index("<http://www.w3.org/ns/shacl#sourceShape>")
    report_iri = report_split[result_index - 1]
    result_iri = report_split[result_index + 1]
    assert result_iri == report_split[shape_index - 1]
    shape_iri = report_split[shape_index + 1]
    return report_iri, result_iri, shape_iri
