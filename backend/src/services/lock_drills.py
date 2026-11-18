from sqlalchemy import select
from redis import Redis
from fastapi import HTTPException
from src.models.entities import LockDrill,LockScenario,Node
from src.config.settings import REDIS_URL
from src.constants.enums import RUN_STATES
from src.utils.serialization import record
from .common import save
from .lock_flow import simulate_lock_drill
queue=Redis.from_url(REDIS_URL,decode_responses=True)
def listing(db):return [record(x) for x in db.scalars(select(LockDrill).order_by(LockDrill.id.desc()))]
def detail(db,ident):
    obj=db.get(LockDrill,ident)
    if not obj:raise HTTPException(404,'锁等待演练不存在')
    return record(obj)
def create(db,data,user):
    scenario=db.get(LockScenario,data.scenario_id)
    if not scenario:raise HTTPException(404,'锁等待场景不存在')
    nodes=[record(x) for x in db.scalars(select(Node).order_by(Node.id))]
    try:simulate_lock_drill(nodes,scenario.events,data.victim_policy,data.max_delay_ms,data.sticky_ms)
    except ValueError as e:raise HTTPException(422,str(e))
    snapshot={'nodes':nodes,'scenario':record(scenario)}
    obj=LockDrill(**data.model_dump(),input_snapshot=snapshot,status=RUN_STATES[0]);result=save(db,obj,user,'锁等待演练入队 '+data.name)
    try:queue.rpush('replicaroute:lock-queue',str(obj.id))
    except Exception:
        obj.status='failed';obj.error='队列暂时不可用，请重新创建';db.commit();raise HTTPException(503,obj.error)
    return result
