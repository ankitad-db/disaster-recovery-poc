"""Genie Conversation API wrapper for the Ask-Genie tab.

Uses the app service principal (CAN RUN on the space). Supports starting a new
conversation or continuing an existing one, and returns the NL answer, any
generated SQL, and the tabular result.
"""
from datetime import timedelta
from typing import Any

from .config import GENIE_SPACE_ID, get_workspace_client


def _extract(msg, space_id: str, conversation_id: str) -> dict[str, Any]:
    """Flatten a completed GenieMessage into answer text + SQL + result table."""
    w = get_workspace_client()
    answer_parts: list[str] = []
    sql: str | None = None
    columns: list[str] = []
    rows: list[list[Any]] = []
    suggestions: list[str] = []

    for att in (msg.attachments or []):
        if att.text and att.text.content:
            answer_parts.append(att.text.content)
        if att.query:
            sql = att.query.query or sql
            if att.query.description:
                answer_parts.append(att.query.description)
            # Fetch the executed result for this query attachment.
            try:
                res = w.genie.get_message_attachment_query_result(
                    space_id, conversation_id, msg.message_id, att.attachment_id
                )
                sr = res.statement_response
                if sr and sr.manifest and sr.manifest.schema:
                    columns = [c.name for c in (sr.manifest.schema.columns or [])]
                if sr and sr.result and sr.result.data_array:
                    rows = sr.result.data_array
            except Exception as e:  # result may be unavailable; keep SQL/answer
                answer_parts.append(f"_(result fetch failed: {e})_")
        if att.suggested_questions and att.suggested_questions.questions:
            for q in att.suggested_questions.questions:
                suggestions.append(q if isinstance(q, str) else getattr(q, "question", str(q)))

    if not answer_parts and msg.content:
        answer_parts.append(msg.content)

    return {
        "conversation_id": conversation_id,
        "message_id": msg.message_id,
        "answer": "\n\n".join(p for p in answer_parts if p).strip()
        or "Genie returned no text answer.",
        "sql": sql,
        "columns": columns,
        "rows": rows,
        "suggestions": suggestions,
        "status": str(msg.status) if msg.status else None,
    }


def ask(question: str, conversation_id: str | None = None) -> dict[str, Any]:
    if not GENIE_SPACE_ID:
        raise RuntimeError("Genie space is not configured (DR_GENIE_SPACE_ID unset).")
    w = get_workspace_client()
    if conversation_id:
        msg = w.genie.create_message_and_wait(
            GENIE_SPACE_ID, conversation_id, question, timeout=timedelta(seconds=280)
        )
    else:
        msg = w.genie.start_conversation_and_wait(
            GENIE_SPACE_ID, question, timeout=timedelta(seconds=280)
        )
        conversation_id = msg.conversation_id

    if msg.error:
        return {
            "conversation_id": conversation_id,
            "message_id": msg.message_id,
            "answer": f"Genie error: {msg.error.error or msg.error}",
            "sql": None, "columns": [], "rows": [], "suggestions": [],
            "status": "ERROR",
        }
    return _extract(msg, GENIE_SPACE_ID, conversation_id)
