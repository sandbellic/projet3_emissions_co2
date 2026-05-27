
from dotenv import load_dotenv
import subprocess

load_dotenv()

#subprocess.run(["dbt", "seed"])
subprocess.run(["dbt", "run"])