import os
from util.water import process_nhdplus_hr_source

process_nhdplus_hr_source(
  base_path=os.path.realpath(__file__),
  url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/"
      "NHDPLUS_H_0107_HU4_GDB.zip",
  gdb_name="NHDPLUS_H_0107_HU4_GDB.gdb"
)
