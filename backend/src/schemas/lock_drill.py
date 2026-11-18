from typing import Literal
from pydantic import BaseModel,Field,model_validator

class LockEvent(BaseModel):
    at_ms:int=Field(ge=0,le=86400000)
    kind:Literal['begin','acquire','check','decide','retry_rollback','commit','rollback','replicate','node_down','node_up','read']
    tx:str|None=Field(default=None,max_length=60)
    resource:str|None=Field(default=None,max_length=40)
    rollback_id:int|None=Field(default=None,ge=1)
    session:str|None=Field(default=None,max_length=60)
    node:str|None=Field(default=None,max_length=80)
    position:int|None=Field(default=None,ge=0,le=1000000000)
    delay_ms:int|None=Field(default=None,ge=0,le=3600000)
    @model_validator(mode='after')
    def valid_event(self):
        if self.kind in ('begin','acquire','commit','rollback') and not self.tx:raise ValueError(self.kind+'事件需要tx')
        if self.kind=='acquire' and not self.resource:raise ValueError('acquire事件需要resource')
        if self.kind=='retry_rollback' and not self.rollback_id:raise ValueError('retry_rollback事件需要rollback_id')
        if self.kind in ('replicate','node_down','node_up') and not self.node:raise ValueError('节点事件需要node')
        if self.kind=='replicate' and (self.position is None or self.delay_ms is None):raise ValueError('replicate需要position和delay_ms')
        return self

class LockScenarioInput(BaseModel):
    code:str=Field(min_length=2,max_length=80)
    title:str=Field(min_length=2,max_length=160)
    events:list[LockEvent]=Field(min_length=1,max_length=120)
    note:str=Field(default='',max_length=300)
    @model_validator(mode='after')
    def chronological(self):
        if any(a.at_ms>b.at_ms for a,b in zip(self.events,self.events[1:])):raise ValueError('事件必须按at_ms非递减排列')
        return self

class LockDrillInput(BaseModel):
    name:str=Field(min_length=2,max_length=120)
    scenario_id:int=Field(gt=0)
    victim_policy:Literal['min_writes','max_blocking','youngest']='min_writes'
    max_delay_ms:int=Field(default=100,ge=0,le=3600000)
    sticky_ms:int=Field(default=500,ge=0,le=3600000)
