import os
import util

# EPSG 4269
url = "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/NHDPLUS_H_1804_HU4_GDB.zip"
dir_name = "NHDPLUS_H_1804_HU4_GDB"
gdb_name = "NHDPLUS_H_1804_HU4_GDB.gdb"

util.process_nhdplus_hr_source(
  os.path.realpath(__file__),
  url,
  dir_name,
  gdb_name
)
