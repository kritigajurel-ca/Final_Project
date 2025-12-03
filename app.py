from flask import Flask, render_template, request
from pymysql import connections
import os
import logging
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# ---------- Configuration ----------
# Database Config
DBHOST = os.environ.get("DBHOST", "localhost")
DBUSER = os.environ.get("DBUSER", "root")
DBPWD  = os.environ.get("DBPWD", "password")
DATABASE = os.environ.get("DATABASE", "employees")
DBPORT = int(os.environ.get("DBPORT", 3306))

# App Config
APP_HEADER = os.environ.get("APP_HEADER", "Group10")

# --- S3 Configuration (Updated for your requirements) ---
# We default to the bucket and image you specified.
# You can still override these using Environment Variables in Kubernetes later if needed.
S3_BUCKET = os.environ.get("S3_BUCKET", "applications3buckect")
S3_IMAGE_KEY = os.environ.get("S3_IMAGE_KEY", "20210116_155141.jpg")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Local storage setup
# We save the downloaded file as 'bg.jpg' so HTML/CSS always looks for the same name
# regardless of what the file is named in S3.
LOCAL_STATIC_DIR = "static"
LOCAL_IMAGE_NAME = "bg.jpg"
LOCAL_IMAGE_PATH = os.path.join(LOCAL_STATIC_DIR, LOCAL_IMAGE_NAME)

# Ensure static folder exists
if not os.path.exists(LOCAL_STATIC_DIR):
    os.makedirs(LOCAL_STATIC_DIR)

# ---------- S3 Download Logic ----------
def download_background_image():
    """
    Attempts to download the specific image from the specific S3 bucket.
    """
    if S3_BUCKET and S3_IMAGE_KEY:
        logging.info(f"Attempting download: s3://{S3_BUCKET}/{S3_IMAGE_KEY}")
        try:
            s3 = boto3.client('s3', region_name=AWS_REGION)
            s3.download_file(S3_BUCKET, S3_IMAGE_KEY, LOCAL_IMAGE_PATH)
            logging.info(f"Successfully downloaded to {LOCAL_IMAGE_PATH}")
            return True
        except ClientError as e:
            logging.error(f"S3 ClientError (Permission/Missing File): {e}")
        except Exception as e:
            logging.error(f"S3 General Error: {e}")
    else:
        logging.warning("S3_BUCKET or S3_IMAGE_KEY not set.")
    
    return False

# Trigger download on app startup
is_downloaded = download_background_image()

# Set the path that HTML templates will use
# If download worked, use 'bg.jpg'. If not, fallback to a default if you have one, or keep it empty.
BG_PATH_FOR_TEMPLATE = LOCAL_IMAGE_NAME if is_downloaded else "default-bg.jpg" 

logging.info(f"Background image to be served: static/{BG_PATH_FOR_TEMPLATE}")

# ---------- Database Connection ----------
db_conn = None
try:
    db_conn = connections.Connection(host=DBHOST, port=DBPORT, user=DBUSER, password=DBPWD, db=DATABASE)
    logging.info("Database connected successfully.")
except Exception as e:
    logging.error(f"Database connection failed: {e}")
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
            logging.error(f"Insert error: {e}")
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
                logging.info(f"No employee found for id: {emp_id}")
        except Exception as e:
            logging.error(f"Fetch error: {e}")
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