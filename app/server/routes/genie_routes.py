"""Ask-Genie endpoint backed by the Genie Conversation API."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import genie
from ..config import GENIE_SPACE_ID

router = APIRouter()


class AskBody(BaseModel):
    question: str
    conversation_id: str | None = None


@router.get("/genie/config")
def genie_config():
    return {"configured": bool(GENIE_SPACE_ID), "space_id": GENIE_SPACE_ID}


@router.post("/genie/ask")
def genie_ask(body: AskBody):
    q = (body.question or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="Question is empty.")
    try:
        return genie.ask(q, body.conversation_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
