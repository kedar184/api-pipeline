# Extractor and Chain Design Pattern

## Core Principles

### 1. Single Responsibility for Base Extractors
- Each base extractor should have a single, well-defined responsibility
- Base extractors should focus on handling a single entity or resource
- They should rely on the base framework for common functionality:
  - Pagination
  - Parallel processing
  - Rate limiting
  - Authentication
  - Retry logic
  - Watermarking

Example: `GitHubCommitsExtractor`
```python
# Simple, focused responsibility
async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Takes a single repo and returns its commits
    repo = parameters["repo"]
    return await self._get_commits_for_repo(repo)
```

### 2. Chain Responsibility
- Chains should handle the orchestration of multiple extractors
- They should manage the flow of data between extractors
- Chains are responsible for:
  - Processing lists of items from previous steps
  - Parallel execution of extractors for multiple items
  - State management between steps
  - Error handling and recovery
  - Data transformation between steps

Example: Chain handling multiple repos
```python
# Chain handles the list processing
async def process_repos_commits(repos: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    commits_by_repo = {}
    for repo in repos:
        extractor = GitHubCommitsExtractor(config)
        commits = await extractor.extract({"repo": repo})
        commits_by_repo[repo] = commits
    return commits_by_repo
```

### 3. Benefits of Separation
1. **Simpler Testing**
   - Base extractors can be tested in isolation
   - Chain logic can be tested independently
   - Easier to mock dependencies

2. **Better Maintainability**
   - Clear separation of concerns
   - Each component has a single responsibility
   - Easier to understand and modify

3. **Improved Reusability**
   - Base extractors can be used in different chains
   - Chains can mix and match extractors
   - Easier to compose new workflows

4. **Scalability**
   - Base extractors focus on efficient single-item processing
   - Chains handle parallelization of multiple items
   - Clear boundaries for optimization

## Implementation Guidelines

### Base Extractor Design
```python
class BaseExtractor:
    async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a single item/resource.
        
        Args:
            parameters: Parameters for the single item
            
        Returns:
            Processed data for the single item
        """
        pass
```

### Chain Design
```python
class Chain:
    async def process_items(self, items: List[Any]) -> List[Dict[str, Any]]:
        """Process multiple items using the extractor.
        
        Args:
            items: List of items from previous step
            
        Returns:
            Combined results from processing all items
        """
        results = []
        async with asyncio.TaskGroup() as group:
            for item in items:
                group.create_task(self._process_single_item(item))
        return results
```

## Example: GitHub Workflow

1. **Base Extractors**
   - `GitHubReposExtractor`: Gets repos for an organization
   - `GitHubCommitsExtractor`: Gets commits for a single repo
   - `GitHubUsersExtractor`: Gets user details

2. **Chain Configuration**
```python
chain_config = {
    "steps": [
        {
            "extractor": GitHubReposExtractor,
            "input": "org_name",
            "output": "repos"
        },
        {
            "extractor": GitHubCommitsExtractor,
            "input": "repos",  # Chain processes this list
            "output": "commits_by_repo"
        }
    ]
}
```

## Migration Strategy

1. **Current State**
   - Extractors handle both single and batch processing
   - Processing patterns mixed with base functionality
   - Complex configuration and setup

2. **Target State**
   - Simplified base extractors
   - Enhanced chain capabilities
   - Clear separation of responsibilities

3. **Steps**
   1. Refactor base extractors to single responsibility
   2. Move list processing to chain layer
   3. Update tests to reflect new pattern
   4. Migrate existing implementations
   5. Update documentation and examples 