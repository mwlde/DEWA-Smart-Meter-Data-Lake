"""
Minimal client for the HBase REST server (standard library only).

HBase's REST server accepts JSON "cell sets": every row key, column name and
value is base64-encoded. Many rows go in one PUT request.

Writes happen on the Spark driver, one request per micro-batch (a few thousand
rows). That is fine at project scale; at DEWA's scale the HBase-Spark connector
or bulk loading would write from the executors instead.
"""

import base64
import json
import urllib.error
import urllib.request

CHUNK = 2000   #rows per request


def _b64(s):
    return base64.b64encode(str(s).encode("utf-8")).decode("ascii")


def check(url, timeout=5):
    """Return None if the REST server answers, else the error text."""
    try:
        with urllib.request.urlopen(f"{url}/version/cluster", timeout=timeout) as r:
            r.read()
        return None
    except Exception as e:
        return str(e)


def table_exists(url, table, timeout=5):
    try:
        with urllib.request.urlopen(f"{url}/{table}/schema", timeout=timeout) as r:
            r.read()
        return True
    except urllib.error.HTTPError:
        return False


def _cell(col, val, ts):
    cell = {"column": _b64(col), "$": _b64(val)}
    if ts is not None:
        cell["timestamp"] = int(ts)   #cell version; without it hbase uses the time of the write
    return cell


def put_rows(url, table, rows, timeout=30):
    """rows: list of (row_key, {"family:qualifier": value}) or (row_key, cells, version_ms).
    Values become strings."""
    for i in range(0, len(rows), CHUNK):
        body = {"Row": [
            {"key": _b64(row[0]),
             "Cell": [_cell(col, val, row[2] if len(row) > 2 else None)
                      for col, val in row[1].items() if val is not None]}
            for row in rows[i:i + CHUNK]
        ]}
        req = urllib.request.Request(
            f"{url}/{table}/batch", data=json.dumps(body).encode("utf-8"), method="PUT",
            headers={"Content-Type": "application/json", "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                r.read()
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")[:300]
            raise RuntimeError(f"HBase REST rejected the write to table '{table}' "
                               f"(HTTP {e.code}): {detail}") from None
        except urllib.error.URLError as e:
            raise RuntimeError(f"Cannot reach HBase REST at {url} while writing to "
                               f"'{table}': {e.reason}") from None
