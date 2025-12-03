from flask import Flask, render_template, request
from pymysql import connections
import os
import logging
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# ---------- Configuration (from env / ConfigMap / Secret) ----------
DBHOST = os.environ.get("DBHOST", "localhost")
DBUSER = os.environ.get("DBUSER", "root")
DBPWD  = os.environ.get("DBPWD", "password")
DATABASE = os.environ.get("DATABASE", "employees")
DBPORT = int(os.environ.get("DBPORT", 3306))

APP_HEADER = os.environ.get("APP_HEADER", "Group10")
# ConfigMap will supply BACKGROUND_IMAGE (s3://bucket/key OR static/images/filename)
BACKGROUND_IMAGE = os.environ.get("BACKGROUND_IMAGE", "static/images/blue-oil-paint-textured-background.jpg")

# Local static image target (Flask serves /static)
LOCAL_STATIC_DIR = "static/images"
LOCAL_FILENAME = os.path.basename(BACKGROUND_IMAGE) if not BACKGROUND_IMAGE.lower().startswith("s3://") else "blue-oil-paint-textured-background.jpg"
LOCAL_IMAGE_PATH = os.path.join(LOCAL_STATIC_DIR, LOCAL_FILENAME)
os.makedirs(LOCAL_STATIC_DIR, exist_ok=True)

# ---------- S3 download (best-effort) ----------
def fetch_from_s3_if_needed(url):
    if not url or not url.lower().startswith("s3://"):
        logging.info("Using local background image path: %s", url)
        return False
    s3_path = url[len("s3://"):]
    parts = s3_path.split("/", 1)
    if len(parts) != 2:
        logging.error("Invalid S3 path: %s", url)
        return False
    bucket, key = parts
    logging.info("Attempting S3 download: s3://%s/%s", bucket, key)
    try:
        s3 = boto3.client("s3")
        s3.download_file(bucket, key, LOCAL_IMAGE_PATH)
        logging.info("Downloaded background to %s", LOCAL_IMAGE_PATH)
        return True
    except ClientError as e:
        logging.error("S3 ClientError: %s", e)
        return False
    except Exception as e:
        logging.error("S3 error: %s", e)
        return False

fetched = fetch_from_s3_if_needed(BACKGROUND_IMAGE)
if fetched:
    BG_PATH_FOR_TEMPLATE = "images/" + LOCAL_FILENAME
else:
    if BACKGROUND_IMAGE.startswith("static/"):
        BG_PATH_FOR_TEMPLATE = BACKGROUND_IMAGE[len("static/"):]
    elif BACKGROUND_IMAGE.startswith("images/"):
        BG_PATH_FOR_TEMPLATE = BACKGROUND_IMAGE
    else:
        BG_PATH_FOR_TEMPLATE = "images/" + LOCAL_FILENAME

logging.info("Background used in templates: %s", BG_PATH_FOR_TEMPLATE)

# ---------- Database connection (best-effort) ----------
db_conn = None
try:
    db_conn = connections.Connection(host=DBHOST, port=DBPORT, user=DBUSER, password=DBPWD, db=DATABASE)
    logging.info("Database connected successfully.")
except Exception as e:
    logging.error("Database connection failed: %s", e)
    db_conn = None

# ---------- Routes ----------
@app.route("/", methods=['GET'])
def home():
    return render_template('addemp.html', header=APP_HEADER, bg_image_path=BG_PATH_FOR_TEMPLATE)

@app.route("/about", methods=['GET'])
def about():
    return render_template('about.html', header=APP_HEADER, bg_image_path=BG_PATH_FOR_TEMPLATE)

@app.route("/addemp", methods=['POST'])
def AddEmp():
    emp_id = request.form.get('emp_id')
    first_name = request.form.get('first_name')
    last_name = request.form.get('last_name')
    primary_skill = request.form.get('primary_skill')
    location = request.form.get('location')
    emp_name = f"{first_name} {last_name}"

    if db_conn:
        try:
            cursor = db_conn.cursor()
            insert_sql = "INSERT INTO employee VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(insert_sql, (emp_id, first_name, last_name, primary_skill, location))
            db_conn.commit()
        except Exception as e:
            logging.error("Insert error: %s", e)
        finally:
            cursor.close()

    return render_template('addempoutput.html', name=emp_name, header=APP_HEADER, bg_image_path=BG_PATH_FOR_TEMPLATE)

@app.route("/getemp", methods=['GET'])
def GetEmp():
    return render_template('getemp.html', header=APP_HEADER, bg_image_path=BG_PATH_FOR_TEMPLATE)

@app.route("/fetchdata", methods=['POST'])
def FetchData():
    emp_id = request.form.get('emp_id')
    output = {}
    if db_conn:
        try:
            cursor = db_conn.cursor()
            select_sql = "SELECT emp_id, first_name, last_name, primary_skill, location FROM employee WHERE emp_id=%s"
            cursor.execute(select_sql, (emp_id,))
            result = cursor.fetchone()
            if result:
                output = {
                    "emp_id": result[0],
                    "first_name": result[1],
                    "last_name": result[2],
                    "primary_skills": result[3],
                    "location": result[4]
                }
            else:
                logging.info("No employee found for id: %s", emp_id)
        except Exception as e:
            logging.error("Fetch error: %s", e)
        finally:
            cursor.close()

    return render_template('getempoutput.html',
                           id=output.get("emp_id"),
                           fname=output.get("first_name"),
                           lname=output.get("last_name"),
                           interest=output.get("primary_skills"),
                           location=output.get("location"),
                           header=APP_HEADER,
                           bg_image_path=BG_PATH_FOR_TEMPLATE)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=81, debug=True)
