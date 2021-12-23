from typing import List, Optional, Dict, Any
import logging
import os
import json
import glob

from fastapi import FastAPI, HTTPException, Header, Response, Body, Depends, status
from fastapi.responses import FileResponse
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.metadata import PaperStatus, Allocation
from app.annotations import Annotation, RelationGroup, PdfAnnotation
from app.utils import StackdriverJsonFormatter
from app import pre_serve

IN_PRODUCTION = os.getenv("IN_PRODUCTION", "dev")

CONFIGURATION_FILE = os.getenv(
    "PAWLS_CONFIGURATION_FILE", "/usr/local/src/skiff/app/api/config/configuration.json"
)

handlers = None

if IN_PRODUCTION == "prod":
    json_handler = logging.StreamHandler()
    json_handler.setFormatter(StackdriverJsonFormatter())
    handlers = [json_handler]

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", default=logging.INFO), handlers=handlers
)
logger = logging.getLogger("uvicorn")

# boto3 logging is _super_ verbose.
logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("nose").setLevel(logging.CRITICAL)
logging.getLogger("s3transfer").setLevel(logging.CRITICAL)

# The annotation app requires a bit of set up.
configuration = pre_serve.load_configuration(CONFIGURATION_FILE)

app = FastAPI()

security = HTTPBasic()




def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    username=credentials.username

    if not user_is_allowed(username):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return  username

def user_is_allowed(user_email: str) -> bool:
    """
    Return True if the user_email is in the users file, False otherwise.
    """
    try:
        with open(configuration.users_file) as file:
            for line in file:
                entry = line.strip()
                if user_email == entry:
                    return True
                # entries like "@allenai.org" mean anyone in that domain @allenai.org is granted access
                if entry.startswith("@") and user_email.endswith(entry):
                    return True
    except FileNotFoundError:
        logger.warning("file not found: %s", configuration.users_file)
        pass

    return False


def all_pdf_shas() -> List[str]:
    pdfs = glob.glob(f"{configuration.output_directory}/*/*.pdf")
    return [p.split("/")[-2] for p in pdfs]


def update_status_json(status_path: str, sha: str, data: Dict[str, Any]):

    with open(status_path, "r+") as st:
        status_json = json.load(st)
        status_json[sha] = {**status_json[sha], **data}
        st.seek(0)
        json.dump(status_json, st)
        st.truncate()


@app.get("/", status_code=204)
def read_root():
    """
    Skiff's sonar, and the Kubernetes health check, require
    that the server returns a 2XX response from it's
    root URL, so it can tell the service is ready for requests.
    """
    return Response(status_code=204)


@app.get("/api/doc/{sha}/pdf")
async def get_pdf(sha: str):
    """
    Fetches a PDF.

    sha: str
        The sha of the pdf to return.
    """
    pdf = os.path.join(configuration.output_directory, sha, f"{sha}.pdf")
    pdf_exists = os.path.exists(pdf)
    if not pdf_exists:
        raise HTTPException(status_code=404, detail=f"pdf {sha} not found.")

    return FileResponse(pdf, media_type="application/pdf")


@app.get("/api/doc/{sha}/title")
async def get_pdf_title(sha: str) -> Optional[str]:
    """
    Fetches a PDF's title.

    sha: str
        The sha of the pdf title to return.
    """
    pdf_info = os.path.join(configuration.output_directory, "pdf_metadata.json")

    with open(pdf_info, "r") as f:
        info = json.load(f)

    data = info.get("sha", None)

    if data is None:
        return None

    return data.get("title", None)


@app.post("/api/doc/{sha}/comments")
def set_pdf_comments(
    sha: str, comments: str = Body(...), user: str = Depends(get_current_username)):
    status_path = os.path.join(configuration.output_directory, "status", f"{user}.json")
    exists = os.path.exists(status_path)

    if not exists:
        # Not an allocated user. Do nothing.
        return {}

    update_status_json(status_path, sha, {"comments": comments})
    return {}


@app.post("/api/doc/{sha}/junk")
def set_pdf_junk(
    sha: str, junk: bool = Body(...), user: str = Depends(get_current_username)):
    status_path = os.path.join(configuration.output_directory, "status", f"{user}.json")
    exists = os.path.exists(status_path)
    if not exists:
        # Not an allocated user. Do nothing.
        return {}

    update_status_json(status_path, sha, {"junk": junk})
    return {}


@app.post("/api/doc/{sha}/finished")
def set_pdf_finished(
    sha: str, finished: bool = Body(...), user: str = Depends(get_current_username)):
    status_path = os.path.join(configuration.output_directory, "status", f"{user}.json")
    exists = os.path.exists(status_path)
    if not exists:
        # Not an allocated user. Do nothing.
        return {}

    update_status_json(status_path, sha, {"finished": finished})
    return {}


@app.get("/api/doc/{sha}/annotations")
def get_annotations(
    sha: str, user: str = Depends(get_current_username)) -> PdfAnnotation:
    annotations = os.path.join(
        configuration.output_directory, sha, f"{user}_annotations.json"
    )
    exists = os.path.exists(annotations)

    if exists:
        with open(annotations) as f:
            blob = json.load(f)

        return blob

    else:
        return {"annotations": [], "relations": []}


@app.post("/api/doc/{sha}/annotations")
def save_annotations(
    sha: str,
    annotations: List[Annotation],
    relations: List[RelationGroup],
user: str = Depends(get_current_username)
):
    """
    sha: str
        PDF sha to save annotations for.
    annotations: List[Annotation]
        A json blob of the annotations to save.
    relations: List[RelationGroup]
        A json blob of the relations between the annotations to save.
    x_auth_request_email: str
        This is a header sent with the requests which specifies the user login.
        For local development, this will be None, because the authentication
        is controlled by the Skiff Kubernetes cluster.
    """
    # Update the annotations in the annotation json file.
    annotations_path = os.path.join(
        configuration.output_directory, sha, f"{user}_annotations.json"
    )
    json_annotations = [jsonable_encoder(a) for a in annotations]
    json_relations = [jsonable_encoder(r) for r in relations]

    # Update the annotation counts in the status file.
    status_path = os.path.join(configuration.output_directory, "status", f"{user}.json")
    exists = os.path.exists(status_path)
    if not exists:
        # Not an allocated user. Do nothing.
        return {}

    with open(annotations_path, "w+") as f:
        json.dump({"annotations": json_annotations, "relations": json_relations}, f)

    update_status_json(
        status_path, sha, {"annotations": len(annotations), "relations": len(relations)}
    )

    return {}


@app.get("/api/doc/{sha}/tokens")
def get_tokens(sha: str):
    """
    sha: str
        PDF sha to retrieve tokens for.
    """
    pdf_tokens = os.path.join(configuration.output_directory, sha, "pdf_structure.json")
    if not os.path.exists(pdf_tokens):
        raise HTTPException(status_code=404, detail="No tokens for pdf.")
    with open(pdf_tokens, "r") as f:
        response = json.load(f)

    return response


@app.get("/api/annotation/labels")
def get_labels() -> List[Dict[str, str]]:
    """
    Get the labels used for annotation for this app.
    """
    return configuration.labels


@app.get("/api/annotation/relations")
def get_relations() -> List[Dict[str, str]]:
    """
    Get the relations used for annotation for this app.
    """
    return configuration.relations


@app.get("/api/annotation/allocation/info")
def get_allocation_info(user: str = Depends(get_current_username)) -> Allocation:

    # In development, the app isn't passed the x_auth_request_email header,
    # meaning this would always fail. Instead, to smooth local development,
    # we always return all pdfs, essentially short-circuiting the allocation
    # mechanism.

    status_dir = os.path.join(configuration.output_directory, "status")
    status_path = os.path.join(status_dir, f"{user}.json")
    exists = os.path.exists(status_path)

    if not exists:
        # If the user doesn't have allocated papers, they can see all the
        # pdfs but they can't save anything.
        papers = [PaperStatus.empty(sha, sha) for sha in all_pdf_shas()]
        response = Allocation(
            papers=papers,
            hasAllocatedPapers=False
        )

    else:
        with open(status_path) as f:
            status_json = json.load(f)

        papers = []
        for sha, status in status_json.items():
            papers.append(PaperStatus(**status))

        response = Allocation(papers=papers, hasAllocatedPapers=True)

    return response
