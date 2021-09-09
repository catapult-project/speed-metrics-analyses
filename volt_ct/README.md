# Setup
Requires python3.

```
$ python3 -m venv volt-ct-env
$ source volt-ct-env/bin/activate
$ pip install google-cloud-{datastore,storage}
```

# Usage
First set `export GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json`

See `python volt_ct.py --help`.
