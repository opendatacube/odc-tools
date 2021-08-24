# Test Docker for odc-tools

1. Download all dependencies, wheels or sources into `/wheels`
2. Compile non-wheels into wheels (parallel compilation is enabled)
3. Build environment from wheels without network access

## Files

- `requirements.txt`
   - Test dependencies
   - Libs with constraints
   - Libs with extra feature flags
   - Optional libs

- `constraints.txt`
   - Auto-generated do not edit manually, see below

- `nobinary.txt`
  - Force compilation from source for selected libs (currently empty)

## Updating constraints.txt

We use `pip-tools` to generate `constraints.txt` from `requirements.txt` file. To get newer versions of libs run this:

```
cd docker
make constraints.txt
```

Then verify that the new set of packages doesn't break things

```
make dkr
make run-test
```

If all is fine check in new version of `constraints.txt`.


### Note on aibotocore

- `pip` attempts to find compatible set of libs
- AWS publishes new boto lib versions every few days, so there are a lot of versions
- `aibotocore` depends on one specific version of `botocore, boto3`
- `pip` has no chance of finding correct set without constraints

This is why in `requirements.txt` we specify `aiobotocore[boto3,awscli]==1.3.3`,
as this limits the search space for `pip-tools` to just a specific version of
`aiobotocore` and a compatible set of boto libs.
