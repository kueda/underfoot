import util
import os
import glob
import subprocess
import csv
import re

work_path = util.make_work_dir(__file__)
os.chdir(work_path)

# download the file if necessary
if not os.path.isfile("sc-geol.e00.gz"):
  url = "http://pubs.usgs.gov/of/1997/of97-489/sc-geol.e00.gz"
  print("Downloading {}\n".format(url))
  util.call_cmd(["curl", "-OL", url])

# extract the archive if necessary
if not os.path.isfile("sc-geol.e00"):
  print("\nExtracting archive...\n")
  util.call_cmd("gunzip -c sc-geol.e00.gz > sc-geol.e00", shell=True)

# # uncompress the e00 itself
# if not os.path.isfile("sfs-geol-uncompressed.e00"):
#   util.call_cmd(["../../bin/e00compr/e00conv", "sfs-geol.e00", "sfs-geol-uncompressed.e00"])

# convert the Arc Info coverages to shapefiles
polygons_path = "sc-geol-shapefiles/polygons.shp"
if not os.path.isfile(polygons_path):
  print("\nConverting e00 to shapefiles...\n")
  shapefiles_path = util.extract_e00("sc-geol.e00")
  polygons_path = util.polygonize_arcs(shapefiles_path)

# dissolve all the shapes by PTYPE and project them into Google Mercator
print("\nDissolving shapes and reprojecting...\n")
final_polygons_path = "polygons.shp"
util.call_cmd([
  "ogr2ogr",
    "-s_srs", "+proj=utm +zone=10 +datum=NAD27 +units=m +no_defs",
    "-t_srs", util.WEB_MERCATOR_PROJ4,
    final_polygons_path, polygons_path,
    "-overwrite",
    "-dialect", "sqlite",
    "-sql", "SELECT PTYPE,ST_Union(geometry) as geometry FROM 'polygons' GROUP BY PTYPE"
])

print("EXTRACTING METADATA...")
metadata_path = "data.csv"
# data = util.metadata_from_usgs_met("nesfgeo/mf2403d.met")
map_unit_data = [
  [
    "Qtl",
    "Colluvium (Holocene)",
    """
      Unconsolidated, heterogeneous deposits of moderately to
      poorly sorted silt, sand, and gravel. Deposited by slope wash and mass movement.
      Minor fluvial reworking. Locally includes numerous landslide deposits and small
      alluvial fans. Contacts generally gradational. Locally grades into fluvial deposits.
      Generally more than 5 ft thick
    """
  ],
  [
    "Qal",
    "Alluvial deposits, undifferentiated (Holocene)",
    """
      Unconsolidated, heterogeneous,
      moderately sorted silt and sand containing discontinuous lenses of clay and silty clay.
      Locally includes large amounts of gravel. May include deposits equivalent to both
      younger (Qyf) and older (Qof) flood-plain deposits in areas where these were not
      differentiated. Thickness highly variable; may be more than 100 ft thick near coast
    """
  ],
  [
    "Qb",
    "Basin deposits (Holocene)",
    """
      Unconsolidated, plastic, silty clay and clay rich in
      organic material. Locally contain interbedded thin layers of silt and silty sand.
      Deposited in a variety of environments including estuaries, lagoons, marsh-filled
      sloughs, flood basins, and lakes. Thickness highly variable; may be as much as 90 ft
      thick underlying some sloughs
    """
  ],
  [
    "Qbs",
    "Beach sand (Holocene)",
    """
      Unconsolidated well-sorted sand. Local layers of pebbles
      and cobbles. Thin discontinuous lenses of silt relatively common in back-beach areas.
      Thickness variable, in part due to seasonal changes in wave energy; commonly less
      than 20 ft thick. May interfinger with either well-sorted dune sand or, where adjacent
      to coastal cliff, poorly-sorted colluvial deposits. Iron-and magnesium-rich heavy
      minerals locally from placers as much as 2 ft thick
    """
  ],
  [
    "Qt",
    "Terrace deposits, undifferentiated (Pleistocene)",
    """
      Weakly consolidated to
      semiconsolidated heterogeneous deposits of moderately to poorly sorted silt, silty
      clay, sand, and gravel. Mostly deposited in a fluvial environment. Thickness highly
      variable; locally as much as 60 ft thick. Some of the deposits are relatively well
      indurated in upper 10 ft of weathered zone
    """
  ],
  [
    "Qes",
    "Eolian deposits of Sunset Beach (Pleistocene)",
    """
      Weakly consolidated, wellsorted,
      fine- to medium-grained sand. Forms an extensive coastal dune field.
      Thickness ranges from 5 to 80 ft
    """
  ],
  [
    "Qcl",
    "Lowest emergent coastal terrace deposits (Pleistocene)",
    """
      Semiconsolidated,
      generally well-sorted sand with a few thin, relatively continuous layers of gravel.
      Deposited in nearshore high-energy marine environment. Grades upward into eolian
      deposits of Manresa Beach in southern part of county. Thickness variable; maximum
      approximately 40 ft. Unit thins to north where it ranges from 5 to 20 ft thick.
      Weathered zone ranges from 5 to 20 ft thick. As mapped, locally includes many small
      areas of fluvial and colluvial silt, sand, and gravel, especially at or near old wave-cut
      cliffs
    """
  ],
  [
    "Qcu",
    "Coastal terrace deposits, undifferentiated (Pleistocene)",
    """
      Semiconsolidated,
      moderately well sorted marine sand with thin, discontinuous gravel-rich layers. May
      be overlain by poorly sorted fluvial and colluvial silt, sand, and gravel. Thickness
      variable; generally less than 20 ft thick. May be relatively well indurated in upper part
      of weathered zone
    """
  ],
  [
    "Qyf",
    "Younger flood-plain deposits (Holocene)",
    """
      Unconsolidated, fine-grained,
      heterogeneous deposits of sand and silt, commonly containing relatively thin,
      discontinuous layers of clay. Gravel content increases toward the Santa Cruz
      Mountains and is locally abundant within channel and lower point-bar deposits in
      natural levees and channels of meandering streams. Thickness generally less than 20
      ft
    """
  ],
  [
    "Qof",
    "Older flood-plain deposits (Holocene)",
    """
      Unconsolidated, fine-grained sand, silt,
      and clay. More than 200 ft thick beneath parts of the Pajaro and San Lorenzo River
      flood plain. Lower parts of these thick fluvial aggradational deposits include large
      amounts of gravel, and serve a major ground-water aquifer beneath Pajaro Valley
    """
  ],
  [
    "Qyfo",
    "Alluvial fan deposits (Holocene)",
    """
      Unconsolidated, moderately to poorly sorted
      sand, silt, and gravel, with layers of silty clay. Generally coarsest nearest the
      mountain front. Thickness uncertain, but may locally be greater than 50 ft
    """
  ],
  [
    "Qds",
    "Dune sand (Holocene)",
    """
      Unconsolidated, well-sorted, fine- to medium-grained sand.
      Deposited as linear strip of coastal dunes. May be as much as 80 ft thick
    """
  ],
  [
    "Qcf",
    "Abandoned channel fill deposits (Holocene)",
    """
      Unconsolidated, plastic, poorly
      sorted clay, silty clay, and silt. Deposited within channels on younger and older
      flood-plain deposits. Thickness generally less than 10 ft
    """
  ],
  [
    "Qem",
    "Eolian deposits of Manresa Beach (Pleistocene)",
    """
      Weakly to moderately
      consolidated, moderately well sorted silt and sand. Deposited in extensive coastal
      dune field. Overlies fluvial terrace deposits (Qwf ). Locally grades conformably into
      underlying coastal terrace deposits (Qcu). Upper 10 to 20 ft is partially indurated
      owing to clay and iron oxide cementation in weathered zone. Moderate permeability
      and porosity except in soil zones, where generally low
    """
  ],
  [
    "Qwf",
    "Fluvial facies (Pleistocene)",
    """
      Semiconsolidated, moderately to poorly sorted silt,
      sand, silty clay, and gravel. May be more than 200 ft thick. Gravel, approximately 50
      ft thick, is generally present 50 ft below surface of deposit and is both a local aquifer
      and significant source of gravel. Upper 5 to 15 ft of unit is moderately indurated
      owing to clay and iron oxide cementation in weathered zone
    """
  ],
  [
    "Qof",
    "Alluvial fan facies (Pleistocene)",
    """
      Semiconsolidated, moderately to poorly
      sorted,discontinuous layers of silty clay, silt, sand, and gravel. Deposited by
      streams, sheet flow, and debris flow on alluvial fans adjacent to Santa Cruz
      Mountains. Thickness variable; locally may be more than 50 ft thick
    """
  ],
  [
    "Qce",
    "Coastal terrace deposits. undifferentiated (Pleistocene)   Eolian facies",
    """
      Semiconsolidated, moderately well sorted eolian sand. Deposited
      conformably on top of coastal terrace deposits; undifferentiated, in western part of
      county. Thickness as much as 40 ft
    """
  ],
  [
    "Qar",
    "Aromas Sand, undivided (Pleistocene)",
    """
      Heterogeneous sequence of mainly eolian
      and fluvial sand, silt, clay, and gravel. Several angular unconformities present in
      unit, with older deposits more complexly jointed, folded, and faulted than younger
      deposits. Total thickness may be more than 800 ft. Locally divided into:
    """
  ],
  [
    "Qae",
    "Aromas Sand, undivided (Pleistocene) Eolian lithofacies",
    """
      Moderately well sorted eolian sand. Highly variable degree of
      consolidation owing to differential weathering. May be as much as 200 ft thick
      without intervening fluvial deposits. Several sequences may be present, separated by
      paleosols. Upper 10 to 20 ft of each dune sequence is oxidized and relatively
      indurated, with all primary structures destroyed by weathering. Lower part of each
      dune sequence below weathering zone may be essentially unconsolidated
    """
  ],
  [
    "Qaf",
    "Aromas Sand, undivided (Pleistocene) Fluvial lithofacies",
    """
      Semiconsolidated, heterogeneous, moderately to poorly sorted
      silty, clay, silt, sand, and gravel. Deposited by meandering and braided streams.
      Includes beds of relatively well sorted gravel ranging from 10 to 20 ft thick. Clay and
      silty clay layers, locally as much as 2 ft thick, occur in unit. Locally includes buried
      soils, high in expansive clays, as much as 14 ft thick
    """
  ],
  [
    "QTc",
    "Continental deposits, undifferentiated (Pleistocene and Pliocene?)",
    """
      Semiconsolidated, fine-grained, oxidized sand and silt. Generally underlie fluvial
      lithofacies of Aromas Sand (Qaf). May represent highly weathered eolian deposits
      formed on Purisima Formation. Thickness approximately 300 ft
    """
  ],
  [
    "Tp",
    "Purisima Formation (Pliocene and upper Miocene)",
    """
      Very thick bedded
      yellowish-gray tuffaceous and diatomaceous siltstone containing thick interbeds of
      bluish-gray, semifriable, fine-grained andesitic sandstone. As shown, includes Santa
      Cruz Mudstone east of Scotts Valley and north of Santa Cruz. Thickness
      approximately 3,000 ft in the Corralitos Canyon area
    """
  ],
  [
    "Tps",
    "Predominantly massive sandstone",
    ""
  ],
  [
    "Tsc",
    "Santa Cruz Mudstone (upper Miocene)",
    """
      Medium-to thick-bedded and faintly
      laminated, blocky-weathering, pale-yellowish-brown siliceous organic mudstone. As
      shown, includes Santa Margarita Sandstone along Glenwood syncline. Thickness at
      least 8,900 ft in the Texas Company Poletti well near Waddell Creek (Clark, 1981,
      p. 31)
    """
  ],
  [
    "Tsm",
    "Santa Margarita Sandstone (upper Miocene)",
    """
      Very thick bedded to massive
      thickly crossbedded yellowish-gray to white friable granular medium-to fine-grained
      arkosic sandstone; locally calcareous and locally bituminous. Thickness 430 ft along
      Scotts Valley syncline (Clark, 1981, p. 25)
    """
  ],
  [
    "Tm",
    "Monterey Formation (middle Miocene)",
    """
      Medium-to thick-bedded and laminated
      olive-gray to light-gray semisiliceous organic mudstone and sandy siltstone. Includes
      a few thick dolomite interbeds. Thickness about 2,675 ft on north limb of Scotts
      Valley syncline (Clark, 1981, p. 21)
    """
  ],
  [
    "Tlo",
    "Lompico Sandstone (middle Miocene)",
    """
      Thick-bedded to massive yellowish-gray,
      medium- to fine-grained calcareous arkosic sandstone; locally friable. Maximum
      thickness 720 ft along Majors Creek (Clark, 1981, p. 18)
    """
  ],
  [
    "Tla",
    "Lambert Shale (lower Miocene)",
    """
      Thin- to medium-bedded and faintly laminated
      olive-gray to dusky-yellowish-brown organic mudstone containing phosphatic
      laminae and lenses in lower part. Thickness about 1,500 ft along Mountain Charlie
      Gulch (Clark, 1981, p. 16)
    """
  ],
  [
    "Tvq",
    "Vaqueros Sandstone (lower Miocene and Oligocene)",
    """
      Thick-bedded to massive
      yellowish-gray arkosic sandstone containing interbeds of olive-gray shale and
      mudstone. Thickness 4,500 ft along Bear Creek (Burchfiel, 1958)
    """
  ],
  [
    "Tbs",
    "Basalt (lower Miocene)",
    """
      Spheroidal-weathering pillow basalt flows. Thickness as
      much as 200 ft along Zayante Road (Clark, 1981, p. 15)
    """
  ],
  [
    "Tz",
    "Zayante Sandstone (Oligocene)",
    """
      Thick- to very thick-bedded, yellowish-orange
      arkosic sandstone containing thin interbeds of greenish and reddish siltstone and
      lenses and thick interbeds of pebble and cobble conglomerate. Thickness 1,800 ft
      along Lompico Creek (Clark, 1981, p. 14)
    """
  ],
  [
    "Tsl",
    "San Lorenzo Formation, undivided (Oligocene and Eocene)",
    ""
  ],
  [
    "Tsr",
    "Rices Mudstone Member (Oligocene and Eocene)",
    """
      Olive-gray mudstone and
      massive medium light-gray, very fine- to fine-grained arkosic sandstone; thick bed of
      glauconitic sandstone at base. Thickness 1,700 ft along Bear Creek (Brabb, 1964, p.
      675)
    """
  ],
  [
    "Tst",
    "Twobar Shale Member (Eocene)",
    """
      Very thin bedded and laminated olive-gray
      shale. Thickness 790 ft along Kings Creek (Brabb, 1964, p. 671).
    """
  ],
  [
    "Tbu",
    "Butano Sandstone (Eocene) Upper sandstone member",
    """
      Thin-bedded to very thick-bedded medium-gray, fine-to
      medium-grained arkosic sandstone containing thin interbeds of medium-gray
      siltstone. Thickness about 3,200 ft (Clark, 1981, p. 8)
    """
  ],
  [
    "Tbm",
    "Butano Sandstone (Eocene) Middle siltstone member",
    """
      Thin- to medium-bedded, nodular, olive-gray pyritic
      siltstone. Thickness about 700 ft (Clark, 1981, p. 8)
    """
  ],
  [
    "Tbl",
    "Butano Sandstone (Eocene) Lower sandstone member",
    """
      Very thick-bedded to massive, yellowish-gray,
      granular, medium- to coarse-grained arkosic sandstone. Thickness as much as 5,000
      ft (Clark, 1981, p. 11)
    """
  ],
  [
    "Tblc",
    "Butano Sandstone (Eocene) Conglomerate",
    """
      Thick to very thick interbeds of sandy pebble conglomerate in lower
      part of lower sandstone member
    """
  ],
  [
    "Tl",
    "Locatelli Formation (Paleocene)",
    """
      Nodular, olive-gray to pale yellowish-brown
      micaceous siltstone. Thickness 800-900 ft (Clark, 1981, p. 7)
    """
  ],
  [
    "Tlss",
    "Locatelli Formation (Paleocene) Sandstone",
    """
      Massive medium-gray, fine-to medium-grained arkosic sandstone locally at
      base. Maximum thickness 80 ft ;Clark, 1981, p. 7)
    """
  ],
  [
    "Ts",
    "Siltstone and sandstone (Pliocene and upper Miocene)",
    """
      Very thick-bedded
      siltstone, sandstone, and minor conglomerate. Referred to the Etchegoin Formation
      by Dibblee and Brabb (1978) and to the Purisima Formation by Allen (1946), who
      indicated that it is nearly 10,000 ft thick. Only about 1,000 ft is exposed in Santa
      Cruz County
    """
  ],
  [
    "Tmp",
    "Shale of Mount Pajaro area (Miocene and Oligocene)",
    """
      Medium- to thickbedded,
      laminated, olive-gray to brownish-black semisiliceous shale, mudstone, and
      less abundant medium-bedded, very pale orange sandstone, tuffaceous sandstone,
      limestone, and conglomerate. Minimum thickness 4,300 ft (Osbun, 1975, p. 1 75)
    """
  ],
  [
    "Tmm",
    "Sandstone of Mount Madonna area (Eocene?)",
    """
      Mostly massive very pale-orange
      arkosic sandstone containing lesser amounts of brownish-black siliceous shale and
      mudstone. Thickness 1,300-2,250 ft (Simon), 1974, p. 45)
    """
  ],
  [
    "Tms",
    "Mudstone of Maymens Flat area (Eocene and Paleocene)",
    """
      Massive dusky
      yellow-green and moderate red mudstone containing abundant planktonic
      foraminifers. In places, mudstone is glauconitic and sandy. Thickness 200-960 ft
      (Simoni), 1974, p. 36)
    """
  ],
  [
    "Kgs",
    "Shale and sandstone of Nibbs Knob area (Upper Cretaceous)",
    """
      Medium- to
      thin-bedded and rhythmically interbedded, olive-black shale and olive-gray sandstone
      (graywacke). Minor thin conglomerate lenses. Thickness 1,100-1,900 ft (Simoni,
      1974, p. 31)
    """
  ],
  [
    "Kcg",
    "Conglomerate (Upper Cretaceous)",
    """
      Consists predominately of well-rounded
      pebbles and cobbles of porphyritic volcanic rocks. Thickness 150-1,120 ft (Simoni,
      1974, p. 25)
    """
  ],
  [
    "qd",
    "Quartz diorite (Cretaceous)",
    "Grades to granodiorite south and east of Ben Lomond Mountain"
  ],
  [
    "ga",
    "Granite and adamellite (Cretaceous)",
    ""
  ],
  [
    "gd",
    "Gneissic granodiorite (Cretaceous)",
    ""
  ],
  [
    "hcg",
    "Hornblende-cummingtonite gabbro (Cretaceous)",
    ""
  ],
  [
    "sch",
    "Metasedimentary rocks (Mesozoic or Paleozoic)",
    "Mainly pelitic schist and quartzite"
  ],
  [
    "m",
    "Marble (Mesozoic or Paleozoic)",
    "Locally contains interbedded schist and calcsilicate rocks"
  ],
  [
    "db",
    "Diabase",
    "Age and stratigraphic relations unknown. Structurally within shale of Mount Pajaro area"
  ]
]

data = [util.METADATA_COLUMN_NAMES]
for row in map_unit_data:
  label_code, label_text, label_desc = row
  rock_name = util.rock_name_from_text(label_text)
  if len(rock_name) == 0:
    rock_name = util.rock_name_from_text(label_desc)
  rock_type = util.rock_type_from_rock_name(rock_name)
  unit = util.unit_from_text(label_text)
  span = util.span_from_text(label_text)
  min_age, max_age, est_age = util.ages_from_span(span)
  data.append([
    label_code,
    label_text,
    re.sub(r"\s+", " ", label_desc).strip(),
    rock_name,
    rock_type,
    unit,
    span,
    min_age,
    max_age,
    est_age
  ])
with open(metadata_path, 'w') as f:
  csv.writer(f).writerows(data)

print("JOINING METADATA...")
util.join_polygons_and_metadata(final_polygons_path, metadata_path)
