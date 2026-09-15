"""Generates water data from TIGER for California"""

from util import tiger_water

FIPS_CODES = [
    # Alameda County
    "06001",
    # Alpine County
    "06003",
    # Amador County
    "06005",
    # Butte County
    "06007",
    # Calaveras County
    "06009",
    # Colusa County
    "06011",
    # Contra Costa County
    "06013",
    # Del Norte County
    "06015",
    # El Dorado County
    "06017",
    # Fresno County
    "06019",
    # Glenn County
    "06021",
    # Humboldt County
    "06023",
    # Imperial County
    "06025",
    # Inyo County
    "06027",
    # Kern County
    "06029",
    # Kings County
    "06031",
    # Lake County
    "06033",
    # Lassen County
    "06035",
    # Los Angeles County
    "06037",
    # Madera County
    "06039",
    # Marin County
    "06041",
    # Mariposa County
    "06043",
    # Mendocino County
    "06045",
    # Merced County
    "06047",
    # Modoc County
    "06049",
    # Mono County
    "06051",
    # Monterey County
    "06053",
    # Napa County
    "06055",
    # Nevada County
    "06057",
    # Orange County
    "06059",
    # Placer County
    "06061",
    # Plumas County
    "06063",
    # Riverside County
    "06065",
    # Sacramento County
    "06067",
    # San Benito County
    "06069",
    # San Bernardino County
    "06071",
    # San Diego County
    "06073",
    # San Francisco County
    "06075",
    # San Joaquin County
    "06077",
    # San Luis Obispo County
    "06079",
    # San Mateo County
    "06081",
    # Santa Barbara County
    "06083",
    # Santa Clara County
    "06085",
    # Santa Cruz County
    "06087",
    # Shasta County
    "06089",
    # Sierra County
    "06091",
    # Siskiyou County
    "06093",
    # Solano County
    "06095",
    # Sonoma County
    "06097",
    # Stanislaus County
    "06099",
    # Sutter County
    "06101",
    # Tehama County
    "06103",
    # Trinity County
    "06105",
    # Tulare County
    "06107",
    # Tuolumne County
    "06109",
    # Ventura County
    "06111",
    # Yolo County
    "06113",
    # Yuba County
    "06115"
]

tiger_water.process_tiger_water_for_fips(FIPS_CODES, source=__file__)
