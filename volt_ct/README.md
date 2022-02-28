# Setup
Requires python3.

```
$ python3 -m venv volt-ct-env
$ source volt-ct-env/bin/activate
$ pip install --upgrade google-cloud-{datastore,storage}
$ pip install -upgrade google-auth-oauthlib
```

# Usage
First set `export VOLT_CT_CLIENT_SECRETS=path/to/client_secrets.json`. See [this](https://docs.google.com/document/d/1uobDbP03hrTWYlaTJRmsd8fu1u1lJe66NFWHZPu8zw0/edit?resourcekey=0-_qq60sf7U5qWWq6_W1IQqg) (internal)

See `python volt_ct.py --help` for other usage.

