# platformstats

Get stats on usage of your development platform tooling.

## Development

1. Install [Python 3.12](https://www.python.org/downloads/).
2. Install dependencies with `pip install -r requirements.txt`.
3. Run the tool via `python main.py`.

And last but not least; set up the tools you want to test and follow the instructions below to extract their logs.

## Artifactory

- Create a support bundle in Artifactory's `/ui/admin/support-zone`.
- Download it.

```bash
platformstats artifactory "${SUPPORT_BUNDLE}.zip"
```
