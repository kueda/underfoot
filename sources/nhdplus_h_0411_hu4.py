import os
from util import extless_basename
from util.water import process_nhdplus_hr_source

NHD_BASENAME = extless_basename(__file__).upper()

process_nhdplus_hr_source(
  os.path.realpath(__file__),
  url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/"
      f"{NHD_BASENAME}_GDB.zip",
  gdb_name=f"{NHD_BASENAME}_GDB.gdb"
)
