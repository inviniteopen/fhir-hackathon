# AGENTS.md

Project-specific instructions for AI assistants.

## Library Versions

This project uses the latest versions of dependencies. Always use current APIs, not deprecated ones.

### Pydantic (v2+)

Use Pydantic v2 APIs:

Documentation: Use Context7 with library ID `/pydantic/pydantic` to query current API usage.

### fhir.resources (v8+)

The `fhir.resources` library is built on Pydantic. Use Pydantic v2 methods for all FHIR model operations.

Documentation: https://github.com/nazrulworld/fhir.resources

## Code Style

- Python 3.11+
- Type hints required
- Use `Path` from `pathlib` for file operations
