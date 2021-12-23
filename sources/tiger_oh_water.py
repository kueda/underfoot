"""Generates water data from TIGER for Ohio"""

from util import tiger_water

FIPS_CODES = [
    # Adam County
    "39001",
    # Alle County
    "39003",
    # Ashlan County
    "39005",
    # Ashtabul County
    "39007",
    # Athens County
    "39009",
    # Auglaize County
    "39011",
    # Belmon County
    "39013",
    # Brow County
    "39015",
    # Butler County
    "39017",
    # Carrol County
    "39019",
    # Champaig County
    "39021",
    # Clar County
    "39023",
    # Clermont County
    "39025",
    # Clinto County
    "39027",
    # Columbiana County
    "39029",
    # Coshocto County
    "39031",
    # Crawford County
    "39033",
    # Cuyahoga County
    "39035",
    # Dark County
    "39037",
    # Defiance County
    "39039",
    # Delaware County
    "39041",
    # Erie County
    "39043",
    # Fairfiel County
    "39045",
    # Fayett County
    "39047",
    # Franklin County
    "39049",
    # Fulton County
    "39051",
    # Gallia County
    "39053",
    # Geauga County
    "39055",
    # Greene County
    "39057",
    # Guernsey County
    "39059",
    # Hamilton County
    "39061",
    # Hancoc County
    "39063",
    # Hardin County
    "39065",
    # Harrison County
    "39067",
    # Henr County
    "39069",
    # Highland County
    "39071",
    # Hockin County
    "39073",
    # Holmes County
    "39075",
    # Huro County
    "39077",
    # Jackso County
    "39079",
    # Jefferso County
    "39081",
    # Knox County
    "39083",
    # Lake County
    "39085",
    # Lawrence County
    "39087",
    # Lickin County
    "39089",
    # Loga County
    "39091",
    # Lorain County
    "39093",
    # Luca County
    "39095",
    # Madiso County
    "39097",
    # Mahoning County
    "39099",
    # Marion County
    "39101",
    # Medina County
    "39103",
    # Meig County
    "39105",
    # Mercer County
    "39107",
    # Miam County
    "39109",
    # Monroe County
    "39111",
    # Montgomery County
    "39113",
    # Morgan County
    "39115",
    # Morrow County
    "39117",
    # Muskingu County
    "39119",
    # Nobl County
    "39121",
    # Ottawa County
    "39123",
    # Paulding County
    "39125",
    # Perr County
    "39127",
    # Pickaway County
    "39129",
    # Pike County
    "39131",
    # Portag County
    "39133",
    # Preble County
    "39135",
    # Putnam County
    "39137",
    # Richland County
    "39139",
    # Ross County
    "39141",
    # Sandusky County
    "39143",
    # Scioto County
    "39145",
    # Seneca County
    "39147",
    # Shelby County
    "39149",
    # Star County
    "39151",
    # Summit County
    "39153",
    # Trumbull County
    "39155",
    # Tuscarawas County
    "39157",
    # Unio County
    "39159",
    # Van Wert County
    "39161",
    # Vinton County
    "39163",
    # Warren County
    "39165",
    # Washington County
    "39167",
    # Wayn County
    "39169",
    # Williams County
    "39171",
    # Wood County
    "39173",
    # Wyando County
    "39175",
]

tiger_water.process_tiger_water_for_fips(FIPS_CODES, source=__file__)
