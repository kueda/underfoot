"""Constants and methods related to projections"""

WEB_MERCATOR_PROJ4 = (
  "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 "
  "+units=m +nadgrids=@null +wktext +over +no_defs"
)
NAD27_UTM10_PROJ4 = "+proj=utm +zone=10 +datum=NAD27 +units=m +no_defs"
NAD27_UTM11_PROJ4 = "+proj=utm +zone=11 +datum=NAD27 +units=m +no_defs"
GRS80_LONGLAT = "+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs"
EPSG_4326_PROJ4 = "+proj=longlat +datum=WGS84 +no_defs"
SRS = EPSG_4326_PROJ4
STATE_PLANE_CA_ZONE_3 = (
  "+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 "
  "+lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs"
)
