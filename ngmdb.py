# https://ngmdb.usgs.gov/ngm-bin/ngm_search_json.pl?State=CA&Counties=Contra%20Costa&Counties=Alameda

import argparse
import re
import requests
from rich.console import Console
from rich.table import Table
from rich.highlighter import RegexHighlighter
from rich.theme import Theme

SEARCH_ENDPOINT = "https://ngmdb.usgs.gov/ngm-bin/ngm_search_json.pl"
STATE_CODES = [
    "AK", "AL", "AR", "AS", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "GU",
     "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN",
     "MO", "MP", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH",
     "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VI", "VT",
     "WA", "WI", "WV", "WY"
]

class GeologicMapHighlighter(RegexHighlighter):
    """Highlight text relative to geologic maps"""
    base_style = "underfoot."
    highlights = [
        re.compile(r"(?P<geologic>geologic|geology)", re.IGNORECASE),
        re.compile(r"(?P<geologic_map>geologic map)", re.IGNORECASE),
        re.compile(r"(?P<database>database)", re.IGNORECASE)
    ]


theme = Theme({
    "underfoot.geologic_map": "bold green",
    "underfoot.geologic": "green",
    "underfoot.database": "bold yellow"
})

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search NGMDB for geologic sources")
    parser.add_argument(
        "--state",
        type=str,
        choices=STATE_CODES,
        help="US state (two-letter)"
    )
    parser.add_argument(
        "--counties",
        type=str,
        action="append"
    )
    args = parser.parse_args()
    print(f"args: {args}")
    response = requests.get(SEARCH_ENDPOINT, {
        "State": args.state,
        "Counties": args.counties,
        "format": "gis"
    })
    # headers = ["title", "scale", "year", "url"]
    # data = [[
    #     result["title"],
    #     result["scale"],
    #     result["year"],
    #     f"https://ngmdb.usgs.gov/Prodesc/proddesc_{result['id']}.htm",

    # ] ]
    # table = PrettyTable()
    # table.field_names = headers
    # table.align["title"] = "l"
    # table.align["url"] = "l"
    # table.add_rows(data)
    # print(table)
    table = Table(title="NGMDB Results", highlight=True)
    table.add_column("title")
    table.add_column("scale", justify="right")
    table.add_column("year", justify="right")
    table.add_column("url")
    for result in response.json()["ngmdb_catalog_search"]["results"]:
        table.add_row(
            result["title"],
            result["scale"],
            str(result["year"]),
            f"https://ngmdb.usgs.gov/Prodesc/proddesc_{result['id']}.htm"
        )
    console = Console(highlighter=GeologicMapHighlighter(), theme=theme)
    console.print(table)
