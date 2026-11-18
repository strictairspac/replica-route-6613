from sqlalchemy import select
from fastapi import HTTPException
from src.models.entities import LockScenario
from src.utils.serialization import record
from .common import save

def _reject_event_noise(events):
    for e in events:
        if e.kind=='replicate' and e.tx:raise HTTPException(422,'replicate事件不接受tx')
        if e.kind=='read' and (e.tx or e.resource):raise HTTPException(422,'read使用session，不接受tx或resource')

def listing(db):return [record(x) for x in db.scalars(select(LockScenario).order_by(LockScenario.id))]
def create(db,data,user):
    _reject_event_noise(data.events);return save(db,LockScenario(**data.model_dump(mode='json')),user,'创建锁等待场景 '+data.code)
def update(db,ident,data,user):
    obj=db.get(LockScenario,ident)
    if not obj:raise HTTPException(404,'锁等待场景不存在')
    _reject_event_noise(data.events)
    for k,v in data.model_dump(mode='json').items():setattr(obj,k,v)
    return save(db,obj,user,'更新锁等待场景 '+data.code)
