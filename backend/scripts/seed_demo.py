from sqlalchemy import select,func
from src.config.database import Session,Base,engine
from src.models.entities import User,Node,RequestSet,Trial,LockScenario
from src.utils.security import password_hash

def seed():
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@qiyuan.test'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,password=password_hash('Demo2026!'),role=role))
        for name,role,pos,delay,note in [('harbor-primary','primary',105,0,'栖原主站 · 写入航道'),('north-replica','replica',104,150,'北站副本 · 延迟偏高'),('south-replica','replica',105,20,'南站副本 · 常用读航道')]:
            if not db.scalar(select(Node).where(Node.name==name)):db.add(Node(name=name,role=role,status='healthy',lineage='qiyuan-log-01',position=pos,delay_ms=delay,note=note))
        mixed=[{'at_ms':0,'kind':'write','session':'lab-A'},{'at_ms':100,'kind':'read','session':'lab-A'},{'at_ms':600,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},{'at_ms':700,'kind':'read','session':'lab-A'},{'at_ms':800,'kind':'node_down','node':'south-replica'},{'at_ms':900,'kind':'read','session':'lab-A'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-A'}]
        cases=[('QY-1117-A','交接班写后读与双节点故障',mixed),('QY-1117-B','延迟超标回退主库',[{'at_ms':0,'kind':'node_down','node':'south-replica'},{'at_ms':10,'kind':'read','session':'visitor'}]),('QY-1117-C','双副本不可读时拒绝',[{'at_ms':0,'kind':'write','session':'lab-B'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-B'}])]
        for code,title,events in cases:
            if not db.scalar(select(RequestSet).where(RequestSet.code==code)):db.add(RequestSet(code=code,title=title,events=events,note='栖原站点值班交接 · 09:15 请求轨迹'))
        deadlock=[{'at_ms':0,'kind':'begin','tx':'tx-A'},{'at_ms':0,'kind':'begin','tx':'tx-B'},
            {'at_ms':10,'kind':'acquire','tx':'tx-A','resource':'orders:42'},{'at_ms':20,'kind':'acquire','tx':'tx-B','resource':'stock:42'},
            {'at_ms':30,'kind':'acquire','tx':'tx-A','resource':'stock:42'},{'at_ms':40,'kind':'acquire','tx':'tx-B','resource':'orders:42'},
            {'at_ms':50,'kind':'check'},{'at_ms':60,'kind':'decide'},
            {'at_ms':70,'kind':'begin','tx':'tx-C'},{'at_ms':80,'kind':'acquire','tx':'tx-C','resource':'orders:42'},
            {'at_ms':90,'kind':'retry_rollback','rollback_id':1},
            {'at_ms':100,'kind':'commit','tx':'tx-A'},{'at_ms':110,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},
            {'at_ms':120,'kind':'read','session':'tx-A'},{'at_ms':130,'kind':'commit','tx':'tx-C'}]
        stale=[{'at_ms':0,'kind':'begin','tx':'tx-P'},{'at_ms':0,'kind':'begin','tx':'tx-Q'},
            {'at_ms':10,'kind':'acquire','tx':'tx-P','resource':'invoice:7'},{'at_ms':20,'kind':'acquire','tx':'tx-Q','resource':'invoice:8'},
            {'at_ms':30,'kind':'acquire','tx':'tx-P','resource':'invoice:8'},{'at_ms':40,'kind':'acquire','tx':'tx-Q','resource':'invoice:7'},
            {'at_ms':50,'kind':'check'},{'at_ms':55,'kind':'rollback','tx':'tx-Q'},{'at_ms':60,'kind':'decide'},
            {'at_ms':70,'kind':'commit','tx':'tx-P'}]
        tradeoff=[{'at_ms':0,'kind':'begin','tx':'tx-A'},{'at_ms':0,'kind':'begin','tx':'tx-B'},{'at_ms':0,'kind':'begin','tx':'tx-C'},{'at_ms':0,'kind':'begin','tx':'tx-D'},
            {'at_ms':10,'kind':'acquire','tx':'tx-A','resource':'orders:42'},{'at_ms':20,'kind':'acquire','tx':'tx-A','resource':'audit:7'},
            {'at_ms':30,'kind':'acquire','tx':'tx-B','resource':'stock:42'},{'at_ms':40,'kind':'acquire','tx':'tx-C','resource':'orders:42'},
            {'at_ms':50,'kind':'acquire','tx':'tx-D','resource':'audit:7'},{'at_ms':60,'kind':'acquire','tx':'tx-B','resource':'orders:42'},
            {'at_ms':70,'kind':'acquire','tx':'tx-A','resource':'stock:42'},{'at_ms':80,'kind':'check'},{'at_ms':90,'kind':'decide'},
            {'at_ms':100,'kind':'retry_rollback','rollback_id':1},
            {'at_ms':140,'kind':'commit','tx':'tx-C'},{'at_ms':150,'kind':'commit','tx':'tx-B'},{'at_ms':200,'kind':'commit','tx':'tx-D'},{'at_ms':210,'kind':'commit','tx':'tx-A'},
            {'at_ms':220,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},{'at_ms':230,'kind':'read','session':'tx-A'}]
        polarized=[{'at_ms':0,'kind':'begin','tx':'tx-B'},{'at_ms':5,'kind':'begin','tx':'tx-A'},
            {'at_ms':10,'kind':'acquire','tx':'tx-B','resource':'shipment:9'},
            {'at_ms':20,'kind':'acquire','tx':'tx-A','resource':'pick:3'},{'at_ms':25,'kind':'acquire','tx':'tx-A','resource':'pick:4'},
            {'at_ms':30,'kind':'acquire','tx':'tx-A','resource':'shipment:9'},{'at_ms':40,'kind':'acquire','tx':'tx-B','resource':'pick:3'},
            {'at_ms':50,'kind':'check'},{'at_ms':60,'kind':'decide'},
            {'at_ms':70,'kind':'commit','tx':'tx-A'},{'at_ms':80,'kind':'commit','tx':'tx-B'}]
        lock_cases=[('QY-LOCK-A','订单与库存双行死锁 · 保全写入',deadlock),('QY-LOCK-B','等待环自行解开 · 旧裁决作废',stale),('QY-LOCK-C','多事务连锁等锁 · 策略代价对照',tradeoff),('QY-LOCK-D','后开始但先做写入 · 三种裁决分叉',polarized)]
        for code,title,events in lock_cases:
            if not db.scalar(select(LockScenario).where(LockScenario.code==code)):db.add(LockScenario(code=code,title=title,events=events,note='交接班写事务互相等锁 · 受控锁管理器模型'))
        db.commit();print({cls.__tablename__:db.scalar(select(func.count()).select_from(cls)) for cls in (User,Node,RequestSet,Trial,LockScenario)})
if __name__=='__main__':seed()
