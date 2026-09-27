"""Generates water data from TIGER for New York"""

from util import tiger_water

FIPS_CODES = [
    # Albany County
    "36001",
    # Allegany County
    "36003",
    # Bronx County
    "36005",
    # Broome County
    "36007",
    # Cattaraugus County
    "36009",
    # Cayuga County
    "36011",
    # Chautauqua County
    "36013",
    # Chemung County
    "36015",
    # Chenango County
    "36017",
    # Clinton County
    "36019",
    # Columbia County
    "36021",
    # Cortland County
    "36023",
    # Delaware County
    "36025",
    # Dutchess County
    "36027",
    # Erie County
    "36029",
    # Essex County
    "36031",
    # Franklin County
    "36033",
    # Fulton County
    "36035",
    # Genesee County
    "36037",
    # Greene County
    "36039",
    # Hamilton County
    "36041",
    # Herkimer County
    "36043",
    # Jefferson County
    "36045",
    # Kings County
    "36047",
    # Lewis County
    "36049",
    # Livingston County
    "36051",
    # Madison County
    "36053",
    # Monroe County
    "36055",
    # Montgomery County
    "36057",
    # Nassau County
    "36059",
    # New York County
    "36061",
    # Niagara County
    "36063",
    # Oneida County
    "36065",
    # Onondaga County
    "36067",
    # Ontario County
    "36069",
    # Orange County
    "36071",
    # Orleans County
    "36073",
    # Oswego County
    "36075",
    # Otsego County
    "36077",
    # Putnam County
    "36079",
    # Queens County
    "36081",
    # Rensselaer County
    "36083",
    # Richmond County
    "36085",
    # Rockland County
    "36087",
    # St. Lawrence County
    "36089",
    # Saratoga County
    "36091",
    # Schenectady County
    "36093",
    # Schoharie County
    "36095",
    # Schuyler County
    "36097",
    # Seneca County
    "36099",
    # Steuben County
    "36101",
    # Suffolk County
    "36103",
    # Sullivan County
    "36105",
    # Tioga County
    "36107",
    # Tompkins County
    "36109",
    # Ulster County
    "36111",
    # Warren County
    "36113",
    # Washington County
    "36115",
    # Wayne County
    "36117",
    # Westchester County
    "36119",
    # Wyoming County
    "36121",
    # Yates County
    "36123",
]

tiger_water.process_tiger_water_for_fips(FIPS_CODES, source=__file__)
