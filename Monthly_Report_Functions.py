# monthly_report_functions.py

import os
import sys
import platform
import yaml
from pathlib import Path
import requests
from utilities import read_config

# Read YAML configurations
conf = read_config("Monthly_Report.yaml")
aws_conf = read_config("Monthly_Report_AWS.yaml")

# Set AWS credentials
os.environ["AWS_ACCESS_KEY_ID"] = aws_conf["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_conf["AWS_SECRET_ACCESS_KEY"]
os.environ["AWS_DEFAULT_REGION"] = aws_conf["AWS_DEFAULT_REGION"]

conf["athena"]["uid"] = aws_conf["AWS_ACCESS_KEY_ID"]
conf["athena"]["pwd"] = aws_conf["AWS_SECRET_ACCESS_KEY"]

# Determine system-specific paths
system_name = platform.system()
if system_name == "Windows":
    home_path = str(Path.home().parent)
    python_path = os.path.join(home_path, "Anaconda3", "python.exe")
elif system_name == "Linux" or system_name == "Darwin":
    home_path = str(Path("~").expanduser())
    python_path = os.path.join(home_path, "miniconda3", "bin", "python")
else:
    raise OSError("Unknown operating system.")

# (Optional) Set default timezone
if platform.node() not in {"GOTO3213490", "Lenny"}:
    os.environ["TZ"] = "America/New_York"

# GDOT proxy config (used in enterprise systems)
if platform.node() in {"GOTO3213490", "Lenny"}:
    import requests
    from requests.auth import HTTPProxyAuth

    proxy_url = "http://gdot-enterprise:8080"
    proxy_auth = HTTPProxyAuth(
        os.getenv("GDOT_USERNAME"), os.getenv("GDOT_PASSWORD")
    )
    proxies = {"http": proxy_url, "https": proxy_url}
    requests_session = requests.Session()
    requests_session.proxies.update(proxies)
    requests_session.auth = proxy_auth
else:
    requests_session = requests.Session()

# Constants (color palette)
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

RED2 = "#e41a1c"
GDOT_BLUE = "#256194"

BLACK = "#000000"
WHITE = "#FFFFFF"
GRAY = "#D0D0D0"
DARK_GRAY = "#7A7A7A"
DARK_DARK_GRAY = "#494949"

# Day of week constants
SUN, MON, TUE, WED, THU, FRI, SAT = range(1, 8)

# Peak hours (from config)
AM_PEAK_HOURS = conf.get("AM_PEAK_HOURS", [])
PM_PEAK_HOURS = conf.get("PM_PEAK_HOURS", [])

# Load utility modules

from utilities import *
from s3_parquet_io import *
from configs import *
from counts import *
from metrics import *
from map import *
from teams import *
from aggregations import *
from database_functions import *

# Number of parallel threads/cores
usable_cores = get_usable_cores()
