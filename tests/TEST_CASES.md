# API Pipeline Test Cases

This document outlines the test cases implemented for the API Pipeline framework.

## Core Components

### Base Classes (`test_core/test_base.py`)

#### BaseExtractor Tests
- **Session Management**
  - Test session initialization
  - Test session cleanup
  - Test header configuration

- **Pagination**
  - Test page number pagination
  - Test cursor-based pagination
  - Test offset pagination
  - Test link header pagination

- **Watermark Handling**
  - Test watermark configuration
  - Test window calculations
  - Test timestamp filtering

#### BaseOutput Tests
- **Initialization**
  - Test configuration validation
  - Test resource allocation
  - Test cleanup

- **Metrics**
  - Test metric collection
  - Test metric updates
  - Test metric reporting

### Pipeline Manager (`test_core/test_pipeline_manager.py`)

- **Pipeline Configuration**
  - Test loading valid configurations
  - Test handling invalid configurations
  - Test environment-specific settings

- **Pipeline Execution**
  - Test successful execution
  - Test error handling
  - Test status updates
  - Test run history

### Factory (`test_core/test_factory.py`)

- **Component Creation**
  - Test extractor instantiation
  - Test output handler registration
  - Test pipeline assembly

- **Secret Resolution**
  - Test secret placeholder replacement
  - Test secret manager integration
  - Test error handling

## Extractors

### Implementation Guide (`test_extractors/test_implementation_guide.py`)

#### Parameter Validation
- `test_parameter_validation`
  - Verifies required parameter checks
  - Tests parameter type validation
  - Tests parameter format validation

#### Data Transformation
- `test_transform_user`
  - Tests user data field mapping
  - Verifies optional field handling
  - Checks data type conversions

- `test_transform_order`
  - Tests order data field mapping
  - Verifies numeric field conversion
  - Tests required field presence

- `test_transform_item_invalid_endpoint`
  - Tests error handling for unsupported endpoints
  - Verifies error message content

#### Watermark Filtering
- `test_watermark_filter`
  - Tests timestamp-based filtering
  - Verifies window calculations
  - Tests record inclusion/exclusion

- `test_watermark_filter_disabled`
  - Tests bypass when watermark is disabled
  - Verifies all records are returned

#### Extraction Process
- `test_extract_method`
  - Tests end-to-end extraction process
  - Verifies API interaction
  - Tests data transformation
  - Checks watermark application

### GitHub Extractor (`test_extractors/test_github_commits.py`)

- **Authentication**
  - Test token authentication
  - Test rate limit handling
  - Test header configuration

- **Data Extraction**
  - Test commit data retrieval
  - Test pull request data retrieval
  - Test error handling

## Output Handlers

### Local JSON Output (`test_outputs/test_local_json.py`)

- **File Operations**
  - Test file creation
  - Test directory structure
  - Test file naming

- **Data Writing**
  - Test JSON serialization
  - Test JSONL format
  - Test compression

- **Partitioning**
  - Test date partitioning
  - Test custom field partitioning
  - Test multi-field partitioning

### BigQuery Output (`test_outputs/test_bigquery.py`)

- **Schema Management**
  - Test schema validation
  - Test schema creation
  - Test schema updates

- **Data Loading**
  - Test batch loading
  - Test streaming inserts
  - Test error handling

## Running Tests

### Local Development
```bash
# Run all tests
python -m pytest

# Run specific test file
python -m pytest tests/extractors/test_implementation_guide.py

# Run with coverage
python -m pytest --cov=api_pipeline

# Run specific test case
python -m pytest tests/extractors/test_implementation_guide.py::test_watermark_filter
```

### Test Configuration

The test suite uses:
- `pytest` for test execution
- `pytest-asyncio` for async test support
- `pytest-cov` for coverage reporting
- Mock objects for external services
- Temporary directories for file operations

### Adding New Tests

When adding new tests:
1. Follow the existing naming convention
2. Include docstrings explaining test purpose
3. Use appropriate fixtures
4. Add test cases to this document
5. Ensure proper error handling coverage 