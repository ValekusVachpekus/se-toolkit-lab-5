"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import distinct, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _lab_title_prefix(lab_id: str) -> str:
    """Convert 'lab-04' → 'Lab 04' for title prefix matching."""
    return "Lab " + lab_id.split("-", 1)[1]


async def _get_task_ids(lab: str, session: AsyncSession) -> tuple[ItemRecord | None, list[int]]:
    """Return the lab ItemRecord and the list of its child task IDs."""
    lab_prefix = _lab_title_prefix(lab)
    lab_item = (
        await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title.startswith(lab_prefix),
            )
        )
    ).first()
    if lab_item is None:
        return None, []
    tasks = (
        await session.exec(
            select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
        )
    ).all()
    return lab_item, [t.id for t in tasks]


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    _, task_ids = await _get_task_ids(lab, session)

    scores = (
        await session.exec(
            select(InteractionLog.score).where(
                InteractionLog.item_id.in_(task_ids),
                InteractionLog.score.is_not(None),
            )
        )
    ).all()

    buckets: dict[str, int] = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    for score in scores:
        if score <= 25:
            buckets["0-25"] += 1
        elif score <= 50:
            buckets["26-50"] += 1
        elif score <= 75:
            buckets["51-75"] += 1
        else:
            buckets["76-100"] += 1

    return [{"bucket": k, "count": v} for k, v in buckets.items()]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    lab_item, _ = await _get_task_ids(lab, session)
    if lab_item is None:
        return []

    result = (await session.exec(
        select(
            ItemRecord.title,
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.parent_id == lab_item.id)
        .group_by(ItemRecord.title)
        .order_by(ItemRecord.title)
    )).all()

    return [
        {
            "task": row.title,
            "avg_score": round(row.avg_score, 1) if row.avg_score is not None else None,
            "attempts": row.attempts,
        }
        for row in result
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    _, task_ids = await _get_task_ids(lab, session)

    rows = (await session.exec(
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )).all()

    return [{"date": str(row.date), "submissions": row.submissions} for row in rows]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    _, task_ids = await _get_task_ids(lab, session)

    rows = (await session.exec(
        select(
            Learner.student_group,
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(distinct(Learner.id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )).all()

    return [
        {
            "group": row.student_group,
            "avg_score": round(row.avg_score, 1) if row.avg_score is not None else None,
            "students": row.students,
        }
        for row in rows
    ]

