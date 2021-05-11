# tap-timebutler

A singer.io tap for extracting data from the timebutler REST API, written in python 3.

API V2 Author: Bryan Mewes (bm@taikonauten.com)

## Quick start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    ```

2. Create your tap's config file which should look like the following:

    ```json
      {
        "auth_token": "your_auth_token"
      }
    ```

3. [Optional] Create the initial state file

    ```json
    {
        "absences": "2000-01-01T00:00:00Z",
        "users": "2000-01-01T00:00:00Z"
    }
    ```

4. Run the application

    `tap-timebutler` can be run with:

    ```bash
    tap-timebutler --config config.json [--state state.json]
    ```

---

Copyright &copy; 2021 Taikonauten
