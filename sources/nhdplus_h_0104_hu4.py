import os
from util.water import process_nhdplus_hr_source

# EPSG 4269
URL = ("https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/"
       "NHDPLUS_H_0104_HU4_GDB.zip")
DIR_NAME = "NHDPLUS_H_0104_HU4_GDB"
GDB_NAME = "NHDPLUS_H_0104_HU4_GDB.gdb"

process_nhdplus_hr_source(
  os.path.realpath(__file__),
  URL,
  DIR_NAME,
  GDB_NAME
)
