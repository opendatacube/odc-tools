# Test Docker for odc-tools

1. Download all dependencies, wheels or sources into `./wheels`
2. Compile non-wheels into wheels (parallel compilation is enabled)
3. Build environment from wheels without network access

Steps 1 and 2 are done in builder docker with output stored in `./wheels`
directory. In step 3 computed wheels are then used to bootstrap environment
without accessing network or needing compilation in the runner docker.

## Files

- `requirements.txt`
   - Test dependencies
   - Libs with constraints
   - Libs with extra feature flags
   - Optional libs

- `constraints.txt`
   - Auto-generated do not edit manually, see below
   - Used in builder stage with `requirements.txt` to pre-compute all the dependencies for `odc.` libs

- `nobinary.txt`
  - Force compilation from source for selected libs (currently empty)

## Updating constraints.txt

### Manually

1. Enter currently working docker with `make bash-runner`
2. Use `python -m pip list --outdated`
3. Update packages you think are worth upgrading `python -m pip install -U <packages>`
4. Verify things are fine `python -m pip check`
5. Generate new `constraints.txt` with `cd docker; make freeze > constraints.txt`


### Attempt automatic upgrade all

In theory you could just

```
echo '' > constraints.txt
make dkr
make freeze > constraints.txt
```

But this might not work because of `boto3` and `aiobotocore`.

- `pip` attempts to find compatible set of libs
- AWS publishes new boto lib versions every few days, so there are a lot of versions
- `aibotocore` depends on one specific version of `botocore, boto3`
- `pip` has no chance of finding correct set without constraints

This is why in `requirements.txt` we specify `aiobotocore[boto3,awscli]==1.3.3`,
as this limits the search space for `pip` to just a specific version of
`aiobotocore` and a compatible set of boto libs.
